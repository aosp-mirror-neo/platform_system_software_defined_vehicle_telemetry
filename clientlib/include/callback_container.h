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

#include "synchronized.h"

#include <stdlib.h>

#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>

namespace google::sdv::telemetry {

template <typename T>
class Callback {
public:
    Callback(void* data, T&& callback) : data(data), callback(callback) {}

    template <typename... A>
    void run(A&&... args) {
        callback(data, std::forward<A>(args)...);
    }

    void* data;

private:
    T callback;
};

// `CallbackContainer` wraps a map of `Callback`s. All methods are thread-safe.
template <typename T>
class CallbackContainer {
public:
    [[nodiscard]]
    bool insert(int apiCallId, Callback<T> callback) {
        return mCallbackMap.lock([&](auto& callbackMap) {
            auto [_, inserted] = callbackMap.emplace(apiCallId, callback);
            return inserted;
        });
    }

    std::optional<Callback<T>> extract(int apiCallId) {
        return mCallbackMap.lock([&](auto& callbackMap) {
            auto nodeHandle = callbackMap.extract(apiCallId);
            return nodeHandle.empty() ? std::nullopt : std::make_optional(nodeHandle.mapped());
        });
    }

private:
    Synchronized<std::unordered_map<int, Callback<T>>> mCallbackMap;
};

}  // namespace google::sdv::telemetry
