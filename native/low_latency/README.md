# Native low-latency execution gate

This C11 sidecar implements the fixed-size V1 binary decision protocol over
colocated UDP. The hot path allocates no memory, performs no logging, and emits
both processing and round-trip telemetry through the Python gateway.

Build and verify on Linux:

```sh
cc -O3 -std=c11 -Wall -Wextra -Werror native_low_latency.c -o funding-native-low-latency
./funding-native-low-latency --self-test 200000
./funding-native-low-latency --listen 0.0.0.0:9010
```

The CI self-test fails when native processing p99 reaches 10 ms. Runtime order
flow remains disabled unless the explicit native policy is enabled and the
rolling processing and round-trip budgets are healthy.
