// Copyright 2024 Google LLC. All Rights Reserved.

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Get the current system time in microseconds as a UNIX timestamp.
int64_t getUnixTimestamp();

// Get the amount of time since boot in microseconds.
int64_t getMonotonicTimeSinceBoot();

#ifdef __cplusplus
}
#endif
