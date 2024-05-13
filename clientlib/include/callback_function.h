// Copyright 2023 Google LLC. All Rights Reserved.

#pragma once

#include <cstdint>

// Define a function prototype for the callback function pointer
using StatusCallbackFunction = void (*)(void*, int32_t, const char*);