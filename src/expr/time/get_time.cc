// Copyright 2024 Google LLC. All Rights Reserved.

#include "get_time.h"

#include <android-base/chrono_utils.h>

#include <stdint.h>

#include <chrono>

int64_t getUnixTimestamp() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
}

int64_t getMonotonicTimeSinceBoot() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
                   ::android::base::boot_clock::now().time_since_epoch())
            .count();
}
