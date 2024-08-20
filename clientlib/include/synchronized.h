// Copyright 2024 Google LLC

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
