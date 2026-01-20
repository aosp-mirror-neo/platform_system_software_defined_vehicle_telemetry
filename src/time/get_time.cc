// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
