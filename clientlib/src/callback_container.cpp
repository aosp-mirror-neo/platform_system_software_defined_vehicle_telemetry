// Copyright 2023 Google LLC. All Rights Reserved.

#define LOG_TAG "CallbackContainer"

#include "callback_container.h"

namespace android::sdv::telemetry {

bool CallbackContainer::insert_callback(int api_call_id, CallbackData cb) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto [_, inserted] = cb_map_.insert({api_call_id, cb});
    return inserted;
}

std::optional<CallbackData> CallbackContainer::extract_callback(int api_call_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto node_handle = cb_map_.extract(api_call_id);
    if (node_handle) {  // Check if node handle is non-empty
        return node_handle.mapped();
    }

    return std::nullopt;
}
}  // namespace android::sdv::telemetry