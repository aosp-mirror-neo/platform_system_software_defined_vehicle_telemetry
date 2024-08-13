// Copyright 2024 Google LLC

#pragma once

#include <stdlib.h>

#include <memory>
#include <mutex>
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
        std::lock_guard<std::mutex> lock(mMutex);
        auto [_, inserted] = mCallbackMap.emplace(apiCallId, callback);
        return inserted;
    }

    std::optional<Callback<T>> extract(int apiCallId) {
        std::lock_guard<std::mutex> lock(mMutex);

        auto nodeHandle = mCallbackMap.extract(apiCallId);
        return nodeHandle.empty() ? std::nullopt : std::make_optional(nodeHandle.mapped());
    }

private:
    std::unordered_map<int, Callback<T>> mCallbackMap;
    std::mutex mMutex;
};

}  // namespace google::sdv::telemetry
