// Copyright 2025 Google LLC

package google.sdv.telemetryapp

import android.util.Log
import google.sdv.telemetry.ITelemetryReportListener
import google.sdv.telemetry.MetricsReportInfo
import google.sdv.telemetry.MetricsReport


interface ReportListenerCallback {
    fun onReport(reportUuid: String, status: Int, statusMessage: String, report: MetricsReport)
}

class TelemetryReportListener(reportUuid: String, callback: ReportListenerCallback) : ITelemetryReportListener.Stub() {
    val mReportUuid = reportUuid
    val TAG = "google.sdv.telemetry.TelemetryApp.ReportListener"
    val mCallback = callback
    private lateinit var mReport: MetricsReport

    override fun onReport(statusCode: Int, statusMessage: String, report: ByteArray) {

        Log.i(TAG, "Received new report: [$statusCode] [$statusMessage]: $mReportUuid")
        mReport = getReport(report)
        mCallback.onReport(mReportUuid, statusCode, statusMessage, mReport)
    }

    override fun getInterfaceHash(): String {
        return ITelemetryReportListener.HASH
    }

    override fun getInterfaceVersion(): Int {
        return ITelemetryReportListener.VERSION
    }

    fun getReport(reportBytes: ByteArray): MetricsReport {
        return MetricsReport.parseFrom(reportBytes)
    }
}
