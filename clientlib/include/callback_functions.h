// Copyright 2023 Google LLC

#pragma once

#include <cstdint>

struct TelemetryServiceStatus;

using GetSerializedDataCallbackFunction = void (*)(void*, int, const char*, const uint8_t*, size_t);
using ConfigListCallbackFunction = void (*)(void*, int, const char*, const char* const*, size_t);
using StatusCallbackFunction = void (*)(void*, int32_t, const char*);
using MiscStatusCallbackFunction = void (*)(void*, const TelemetryServiceStatus*);
using ReportReadyCallbackFunction = void (*)(void*, const char*);
using ReadyReportsInfoCallbackFunction = void (*)(void*, int, const char*, const char* const*,
                                                  size_t);