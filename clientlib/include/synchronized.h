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

#pragma once

#include <mutex>

// Thin wrapper around a `T` that guards all access to it with a `std::mutex`.
// To access `T`, call `Synchronized::lock` with a lambda that accepts a `T&` or
// `const T&`.
//
// The benefit of this abstraction over a `T` with a separate `std::mutex` is
// that you can never forget to lock the mutex when accessing `T`.
//
// The name is based on the more powerful version of this class in `folly`.
template <typename T>
class Synchronized {
public:
    auto lock(auto&& f) {
        std::lock_guard<std::mutex> guard(mMutex);
        return f(mObject);
    }

    auto lock(auto&& f) const {
        std::lock_guard<std::mutex> guard(mMutex);
        return f(mObject);
    }

private:
    mutable std::mutex mMutex;
    T mObject;
};
