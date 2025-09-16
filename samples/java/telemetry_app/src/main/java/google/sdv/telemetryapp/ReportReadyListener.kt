// Copyright 2025 Google LLC

package google.sdv.telemetryapp

import android.util.Log
import google.sdv.telemetry.ITelemetryReportReadyListener
import google.sdv.telemetry.MetricsReportInfo

interface ReportReadyListenerCallback {
    fun onReportReady(reportInfo: MetricsReportInfo)
}

class TelemetryReadyReportListener(callId: Int, callback: ReportReadyListenerCallback) : ITelemetryReportReadyListener.Stub() {
    val mCallId = callId
    val TAG = "google.sdv.telemetry.TelemetryApp.ReportReadyListener"
    val mCallback = callback

    val reportMap = LinkedHashMap<String, MetricsReportInfo>()

    override fun onReady(reportInfo: MetricsReportInfo) {
        Log.i(TAG, "Received new report info: " + reportInfo.reportUuid + " for config uuid:" + reportInfo.configUuid)
        reportMap.put(reportInfo.configUuid + "_" + reportInfo.reportUuid, reportInfo)
        mCallback.onReportReady(reportInfo)
    }

    override fun getInterfaceHash(): String {
        return ITelemetryReportReadyListener.HASH
    }

    override fun getInterfaceVersion(): Int {
        return ITelemetryReportReadyListener.VERSION
    }
}
