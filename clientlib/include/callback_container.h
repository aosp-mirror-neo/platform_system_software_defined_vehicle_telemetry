// Copyright 2024 Google LLC

#ifndef TELEMETRY_CLIENTLIB_INCLUDE_CALLBACK_CONTAINER_H
#define TELEMETRY_CLIENTLIB_INCLUDE_CALLBACK_CONTAINER_H

#include "callback_functions.h"

#include <utils/Log.h>

#include <stdlib.h>

#include <memory>
#include <mutex>
#include <unordered_map>

namespace google::sdv::telemetry {

// Define the struct
struct CallbackData {
    void* cb_object;
    StatusCallbackFunction cb_function;
};

// Acts as the mediator between the OEM client and the telemetry service.
class StatusCallbackContainer {
public:
    bool insert_callback(int api_call_id, CallbackData);
    std::optional<CallbackData> extract_callback(int api_call_id);

private:
    std::unordered_map<int, CallbackData> cb_map_;
    std::mutex mutex_;
};
}  // namespace google::sdv::telemetry

#endif  // TELEMETRY_CLIENTLIB_INCLUDE_CALLBACK_CONTAINER_H
