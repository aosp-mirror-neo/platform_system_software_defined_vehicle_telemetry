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
