// Copyright 2025 Google LLC

package google.sdv.telemetryapp

import android.os.Bundle
import android.os.IBinder
import android.util.Log
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.runtime.snapshots.SnapshotStateMap
import google.sdv.telemetryapp.ui.theme.TelemetryAppTheme
import google.sdv.telemetryapp.ui.TelemetryApp
import google.sdv.telemetry.ITelemetryService
import google.sdv.telemetry.ITelemetryReportReadyListener
import google.sdv.telemetry.MetricsReportInfo
import google.sdv.telemetry.MetricsReport
import google.sdv.telemetry.java.Binder.BinderServiceAccessor;
import java.io.File
import java.io.FileInputStream
import java.io.IOException

class MainActivity : ComponentActivity(), ReportReadyListenerCallback, ReportListenerCallback {
    private lateinit var mTelemetryService: ITelemetryService
    private lateinit var mReportReadyListener: TelemetryReadyReportListener
    private lateinit var mReportMap: SnapshotStateMap<String, MetricsReportInfo>
    private var mReportListener: TelemetryReportListener? = null
    private var mDetailedReport by mutableStateOf<MetricsReport?>(null)

    var mApiCallId = 1

    val ITELEMETRY_SERVICE_URI =
            "google.sdv.telemetry.ITelemetryService/default"
    val TAG = "google.sdv.telemetry.TelemetryApp"
    private val CONFIG_FILE = "/data/local/tmp/config.pbin"


    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        try {
            initTelemetryService()
        }
        catch(e: IllegalStateException) {
            Log.e(TAG, "Could not get ITelemetryService: " + e.toString())
        }

        mReportMap = mutableStateMapOf()
        //TODO: b/444371163: Implement Telemetry Status Listeners
        var callId = getCallId()
        mReportReadyListener = TelemetryReadyReportListener(callId, this)
        mTelemetryService.registerReportReadyListener(callId, mReportReadyListener)

        setContent {
            TelemetryAppTheme {
                TelemetryApp(
                    onLoadConfig = { filePath ->
                        val configData = readConfigFile(if (filePath.isNotEmpty()) filePath else CONFIG_FILE)
                        if (configData != null) {
                            Log.d(TAG, "Successfully read config file")
                            val callId = getCallId()
                            mTelemetryService.addMetricsConfig(callId, configData)
                        } else {
                            Log.e(TAG, "Failed to read config file")
                        }
                    },
                    onGetConfigs = { active, inactive ->
                        val callIdActive = getCallId()
                        val activeListener = TelemetryConfigListener(callIdActive, active)
                        mTelemetryService.getActiveMetricsConfigUuids(activeListener)

                        val callIdInactive = getCallId()
                        val inactiveListener = TelemetryConfigListener(callIdInactive, inactive)
                        mTelemetryService.getInactiveMetricsConfigUuids(inactiveListener)
                    },
                    onActivateConfig = { config ->
                        val callId = getCallId()
                        mTelemetryService.activateMetricsConfig(callId, config)
                    },
                    onDeactivateConfig = { config ->
                        val callId = getCallId()
                        mTelemetryService.deactivateMetricsConfig(callId, config)
                    },
                    reports = mReportMap,
                    onGetReport = { reportUuid ->
                        getMetricsReport(reportUuid)
                    },
                    detailedReport = mDetailedReport,
                    onBack = { mDetailedReport = null }
                )
            }
        }
    }

    override fun onDestroy() {
        super.onDestroy()
        mTelemetryService.unregisterReportReadyListener()
    }

    override fun onReportReady(reportInfo: MetricsReportInfo) {
        mReportMap.put(reportInfo.configUuid + "_" + reportInfo.reportUuid, reportInfo)
    }

    override fun onReport(reportUuid: String, status: Int, statusMessage: String, report: MetricsReport) {
        mDetailedReport = report
    }

    fun getMetricsReport(reportUuid: String) {
        mReportListener = TelemetryReportListener(reportUuid, this)
        mTelemetryService.getMetricsReport(reportUuid, mReportListener)
    }

    fun initTelemetryService(){

        System.loadLibrary("binder_accessor");
        val regService = BinderServiceAccessor
            .getService(ITELEMETRY_SERVICE_URI);


        // Check if the service was actually found. If not, throw an exception.
        if (regService == null) {
            throw IllegalStateException(
                    "TelemetryService service was not found. It may not be"
                            + " running.");
        }

        mTelemetryService =
                ITelemetryService.Stub.asInterface(regService);
        Log.i(TAG, "Connected to ITelemetryService interface.")
    }

    fun getCallId(): Int {
        return ++mApiCallId
    }

    fun readConfigFile(filePath: String): ByteArray? {
        val file = File(filePath)
        if (!file.exists()) {
            Log.e(TAG, "Config file not found at $filePath")
            return null
        }

        return try {
            val fileInputStream = FileInputStream(file)
            val bytes = fileInputStream.readBytes()
            fileInputStream.close()
            bytes
        } catch (e: IOException) {
            Log.e(TAG, "Error reading config file", e)
            null
        }
    }
}
