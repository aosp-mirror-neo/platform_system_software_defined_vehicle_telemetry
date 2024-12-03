// Copyright 2023 Google LLC

#pragma once

#include <cstdint>

struct MetricsReportInfo;

using GetSerializedDataCallbackFunction = void (*)(void*, int, const char*, const uint8_t*, size_t);
using ConfigListCallbackFunction = void (*)(void*, int, const char*, const char**, size_t);
using StatusCallbackFunction = void (*)(void*, int32_t, const char*);
using ReportReadyCallbackFunction = void (*)(void*, const char*, const char*, int32_t);
using ReadyReportsInfoCallbackFunction = void (*)(void*, int, const char*, const MetricsReportInfo*,
                                                  size_t);