// Copyright 2025 Google LLC

package google.sdv.telemetryapp

import android.util.Log
import google.sdv.telemetry.ITelemetryConfigListListener

class TelemetryConfigListener(
    callId: Int,
    private val configs: MutableList<String>
) : ITelemetryConfigListListener.Stub() {
    val mCallId = callId
    val TAG = "google.sdv.telemetry.TelemetryApp.ITelemetryConfigListListener"


    override fun sendConfigList(statusCode: Int, statusMessage: String, configList: List<String>) {
        Log.i(TAG, "ConfigList Callback: " + statusCode + " Message: " + statusMessage)
        Log.i(TAG, "Configurations available: " + configList.joinToString(separator = ", "))
        configs.addAll(configList)
    }

    override fun getInterfaceHash(): String {
        return ITelemetryConfigListListener.HASH
    }

    override fun getInterfaceVersion(): Int {
        return ITelemetryConfigListListener.VERSION
    }

}
