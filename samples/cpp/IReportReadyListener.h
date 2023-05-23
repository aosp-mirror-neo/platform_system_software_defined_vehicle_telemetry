// Copyright 2023 Google LLC. All Rights Reserved.

#include <stdlib.h>
#include <stdio.h>
#include <utils/Log.h>
#include <utils/RefBase.h>

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
    void onReady(const std::string& metricConfigName) override {
        ALOGI("Report Ready for config: %s", metricConfigName.c_str());
    }
};

