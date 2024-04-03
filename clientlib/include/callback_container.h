// Copyright 2023 Google LLC. All Rights Reserved.

#ifndef TELEMETRY_CLIENTLIB_INCLUDE_CALLBACK_CONTAINER_H
#define TELEMETRY_CLIENTLIB_INCLUDE_CALLBACK_CONTAINER_H

#include <utils/Log.h>
#include <utils/RefBase.h>

#include <stdlib.h>

#include <memory>
#include <mutex>
#include <unordered_map>

namespace google::sdv::telemetry {

// Define a function prototype for the callback function pointer
typedef void (*CbFunction)(void*, int32_t, const char*);

// Define the struct
struct CallbackData {
    void* cb_object;
    CbFunction cb_function;
};

// Acts as the mediator between the OEM client and the telemetry service.
class CallbackContainer : public android::RefBase {
public:
    bool insert_callback(int api_call_id, CallbackData);
    std::optional<CallbackData> extract_callback(int api_call_id);

private:
    std::unordered_map<int, CallbackData> cb_map_;
    std::mutex mutex_;
};
}  // namespace google::sdv::telemetry

#endif  // TELEMETRY_CLIENTLIB_INCLUDE_CALLBACK_CONTAINER_H
