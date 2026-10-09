#define _POSIX_C_SOURCE 200809L

#include <arpa/inet.h>
#include <errno.h>
#include <inttypes.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define NATIVE_MAGIC UINT32_C(0x31544C4E)
#define NATIVE_VERSION UINT16_C(1)
#define STATUS_ACCEPTED UINT16_C(1)
#define STATUS_REJECTED_COST UINT16_C(2)
#define STATUS_INVALID UINT16_C(3)
#define SIDE_BUY UINT16_C(1)
#define SIDE_SELL UINT16_C(2)
#define BPS_SCALE 10000.0L
#define PRICE_BPS 10000.0L
#define HARD_PROCESSING_BUDGET_NS UINT64_C(10000000)

#pragma pack(push, 1)
typedef struct {
    uint32_t magic;
    uint16_t version;
    uint16_t side;
    uint64_t sequence;
    int64_t quantity_e8;
    int64_t reference_price_e8;
    int64_t market_price_e8;
    int64_t fee_bps_e4;
    int64_t maximum_slippage_bps_e4;
    uint64_t sent_monotonic_ns;
} native_request_t;

typedef struct {
    uint32_t magic;
    uint16_t version;
    uint16_t status;
    uint64_t sequence;
    int64_t limit_price_e8;
    int64_t all_in_cost_bps_e4;
    uint64_t processing_ns;
    uint64_t echoed_sent_monotonic_ns;
} native_response_t;
#pragma pack(pop)

_Static_assert(sizeof(native_request_t) == 64, "native request wire size changed");
_Static_assert(sizeof(native_response_t) == 48, "native response wire size changed");

static volatile sig_atomic_t running = 1;

static uint64_t monotonic_ns(void) {
    struct timespec value;
    if (clock_gettime(CLOCK_MONOTONIC_RAW, &value) != 0) {
        return 0;
    }
    return (uint64_t)value.tv_sec * UINT64_C(1000000000) + (uint64_t)value.tv_nsec;
}

static void stop_server(int signal_number) {
    (void)signal_number;
    running = 0;
}

static native_response_t evaluate(const native_request_t *request) {
    const uint64_t started = monotonic_ns();
    native_response_t response;
    memset(&response, 0, sizeof(response));
    response.magic = NATIVE_MAGIC;
    response.version = NATIVE_VERSION;
    response.sequence = request->sequence;
    response.echoed_sent_monotonic_ns = request->sent_monotonic_ns;
    if (request->magic != NATIVE_MAGIC || request->version != NATIVE_VERSION ||
        (request->side != SIDE_BUY && request->side != SIDE_SELL) ||
        request->quantity_e8 <= 0 || request->reference_price_e8 <= 0 ||
        request->market_price_e8 <= 0 || request->fee_bps_e4 < 0 ||
        request->maximum_slippage_bps_e4 < 0) {
        response.status = STATUS_INVALID;
        response.processing_ns = monotonic_ns() - started;
        return response;
    }

    long double adverse_price = 0.0L;
    if (request->side == SIDE_BUY &&
        request->market_price_e8 > request->reference_price_e8) {
        adverse_price = (long double)(request->market_price_e8 - request->reference_price_e8);
    } else if (request->side == SIDE_SELL &&
               request->market_price_e8 < request->reference_price_e8) {
        adverse_price = (long double)(request->reference_price_e8 - request->market_price_e8);
    }
    const long double slippage_bps_e4 =
        adverse_price * PRICE_BPS * BPS_SCALE /
        (long double)request->reference_price_e8;
    const int64_t all_in_cost =
        (int64_t)(slippage_bps_e4 + (long double)request->fee_bps_e4 + 0.5L);
    response.all_in_cost_bps_e4 = all_in_cost;
    if (all_in_cost <= request->maximum_slippage_bps_e4) {
        response.status = STATUS_ACCEPTED;
        response.limit_price_e8 = request->market_price_e8;
    } else {
        response.status = STATUS_REJECTED_COST;
    }
    response.processing_ns = monotonic_ns() - started;
    return response;
}

static int compare_u64(const void *left, const void *right) {
    const uint64_t a = *(const uint64_t *)left;
    const uint64_t b = *(const uint64_t *)right;
    return (a > b) - (a < b);
}

static int self_test(size_t iterations) {
    uint64_t *samples = calloc(iterations, sizeof(*samples));
    if (samples == NULL) {
        fprintf(stderr, "native benchmark allocation failed\n");
        return 2;
    }
    native_request_t request = {
        .magic = NATIVE_MAGIC,
        .version = NATIVE_VERSION,
        .side = SIDE_BUY,
        .sequence = 1,
        .quantity_e8 = INT64_C(100000000),
        .reference_price_e8 = INT64_C(10000000000),
        .market_price_e8 = INT64_C(10001000000),
        .fee_bps_e4 = INT64_C(10000),
        .maximum_slippage_bps_e4 = INT64_C(500000),
        .sent_monotonic_ns = 0,
    };
    for (size_t index = 0; index < iterations; ++index) {
        request.sequence = (uint64_t)index + 1;
        const native_response_t response = evaluate(&request);
        if (response.status != STATUS_ACCEPTED) {
            free(samples);
            fprintf(stderr, "native benchmark decision unexpectedly rejected\n");
            return 3;
        }
        samples[index] = response.processing_ns;
    }
    qsort(samples, iterations, sizeof(*samples), compare_u64);
    const size_t p50_index = (iterations - 1) * 50 / 100;
    const size_t p95_index = (iterations - 1) * 95 / 100;
    const size_t p99_index = (iterations - 1) * 99 / 100;
    const uint64_t p99 = samples[p99_index];
    printf(
        "{\"iterations\":%zu,\"processing_p50_ns\":%" PRIu64
        ",\"processing_p95_ns\":%" PRIu64 ",\"processing_p99_ns\":%" PRIu64
        ",\"processing_max_ns\":%" PRIu64 ",\"budget_ns\":%" PRIu64
        ",\"pass\":%s}\n",
        iterations,
        samples[p50_index],
        samples[p95_index],
        p99,
        samples[iterations - 1],
        HARD_PROCESSING_BUDGET_NS,
        p99 <= HARD_PROCESSING_BUDGET_NS ? "true" : "false"
    );
    free(samples);
    return p99 <= HARD_PROCESSING_BUDGET_NS ? 0 : 4;
}

static int serve(const char *address) {
    char host[64];
    const char *separator = strrchr(address, ':');
    if (separator == NULL || separator == address) {
        fprintf(stderr, "listen address must use IPv4:port\n");
        return 2;
    }
    const size_t host_length = (size_t)(separator - address);
    if (host_length >= sizeof(host)) {
        fprintf(stderr, "listen host is too long\n");
        return 2;
    }
    memcpy(host, address, host_length);
    host[host_length] = '\0';
    char *port_end = NULL;
    const long port = strtol(separator + 1, &port_end, 10);
    if (port_end == separator + 1 || *port_end != '\0' || port <= 0 || port > 65535) {
        fprintf(stderr, "listen port is invalid\n");
        return 2;
    }

    const int server = socket(AF_INET, SOCK_DGRAM, 0);
    if (server < 0) {
        perror("socket");
        return 2;
    }
    const int reuse = 1;
    if (setsockopt(server, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) != 0) {
        perror("setsockopt");
        close(server);
        return 2;
    }
    struct sockaddr_in endpoint;
    memset(&endpoint, 0, sizeof(endpoint));
    endpoint.sin_family = AF_INET;
    endpoint.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &endpoint.sin_addr) != 1) {
        fprintf(stderr, "listen host must be an IPv4 address\n");
        close(server);
        return 2;
    }
    if (bind(server, (const struct sockaddr *)&endpoint, sizeof(endpoint)) != 0) {
        perror("bind");
        close(server);
        return 2;
    }
    signal(SIGINT, stop_server);
    signal(SIGTERM, stop_server);
    fprintf(stdout, "native-low-latency listening on %s\n", address);
    fflush(stdout);
    while (running) {
        native_request_t request;
        struct sockaddr_in client;
        socklen_t client_length = sizeof(client);
        const ssize_t received = recvfrom(
            server,
            &request,
            sizeof(request),
            0,
            (struct sockaddr *)&client,
            &client_length
        );
        if (received < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("recvfrom");
            close(server);
            return 2;
        }
        if ((size_t)received != sizeof(request)) {
            continue;
        }
        const native_response_t response = evaluate(&request);
        if (sendto(
                server,
                &response,
                sizeof(response),
                0,
                (const struct sockaddr *)&client,
                client_length
            ) != (ssize_t)sizeof(response)) {
            perror("sendto");
        }
    }
    close(server);
    return 0;
}

int main(int argc, char **argv) {
    if (argc >= 2 && strcmp(argv[1], "--self-test") == 0) {
        size_t iterations = 200000;
        if (argc == 3) {
            char *end = NULL;
            const unsigned long long parsed = strtoull(argv[2], &end, 10);
            if (end == argv[2] || *end != '\0' || parsed < 1000) {
                fprintf(stderr, "self-test iterations must be at least 1000\n");
                return 2;
            }
            iterations = (size_t)parsed;
        }
        return self_test(iterations);
    }
    if (argc == 3 && strcmp(argv[1], "--listen") == 0) {
        return serve(argv[2]);
    }
    fprintf(stderr, "usage: %s --self-test [iterations] | --listen IPv4:port\n", argv[0]);
    return 2;
}
