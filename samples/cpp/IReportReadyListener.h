// Copyright 2023 Google LLC. All Rights Reserved.

#include <utils/Log.h>
#include <utils/RefBase.h>

#include <stdio.h>
#include <stdlib.h>

#include <string>

class IReportReadyListener {
  public:
    virtual void onReady(const std::string& data) = 0;
    virtual ~IReportReadyListener(){
        ALOGI("IReportReadyListener Destroyed");
    }
};

class ReportReadyImpl final:
      public IReportReadyListener, public android::RefBase {
public:
    void onReady(const std::string& metricConfigUuid) override {
        ALOGI("Report Ready for config: %s", metricConfigUuid.c_str());
    }
};

