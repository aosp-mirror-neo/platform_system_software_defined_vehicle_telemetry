// Copyright 2023 Google LLC

#define LOG_TAG "StatusCallbackContainer"

#include "callback_container.h"

namespace google::sdv::telemetry {

bool StatusCallbackContainer::insert_callback(int api_call_id, CallbackData cb) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto [_, inserted] = cb_map_.insert({api_call_id, cb});
    return inserted;
}

std::optional<CallbackData> StatusCallbackContainer::extract_callback(int api_call_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto node_handle = cb_map_.extract(api_call_id);
    if (node_handle) {  // Check if node handle is non-empty
        return node_handle.mapped();
    }

    return std::nullopt;
}
}  // namespace google::sdv::telemetry
