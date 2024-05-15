// Copyright 2023 Google LLC. All Rights Reserved.

#pragma once

#include <cstdint>

// Define a function prototype for the callback function pointer
using GetSerializedDataCallbackFunction = void (*)(void*, int, const char*, const uint8_t*, size_t);
using ConfigListCallbackFunction = void (*)(void*, int, const char*, const char**, size_t);
using StatusCallbackFunction = void (*)(void*, int32_t, const char*);