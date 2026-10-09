cmake_minimum_required(VERSION 3.20)
project(funding_native_low_latency C)

add_executable(funding-native-low-latency native_low_latency.c)
target_compile_features(funding-native-low-latency PRIVATE c_std_11)
target_compile_options(funding-native-low-latency PRIVATE -O3 -Wall -Wextra -Werror)
