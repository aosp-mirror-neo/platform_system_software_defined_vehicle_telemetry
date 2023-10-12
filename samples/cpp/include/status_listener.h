// Copyright 2023 Google LLC. All Rights Reserved.

#include <utils/Log.h>
#include <utils/RefBase.h>

#include <stdio.h>
#include <stdlib.h>

#include <string>

class StatusListenerImpl final : public android::RefBase {
public:
    void onStatus(int status_code, const std::string& status_message) {
        ALOGI("status code: %d, status_message: %s", status_code, status_message.c_str());
    }
};
