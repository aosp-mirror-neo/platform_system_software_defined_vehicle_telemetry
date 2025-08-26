// Copyright 2025 Google LLC

package google.sdv.telemetryapp.ui

import google.sdv.telemetry.MetricsReport
import google.sdv.telemetry.MetricsReportInfo
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.runtime.snapshots.SnapshotStateMap
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun TelemetryApp(
    onLoadConfig: (String) -> Unit,
    onGetConfigs: (MutableList<String>, MutableList<String>) -> Unit,
    onActivateConfig: (String) -> Unit,
    onDeactivateConfig: (String) -> Unit,
    reports: SnapshotStateMap<String, MetricsReportInfo>,
    onGetReport: (String) -> Unit,
    detailedReport: MetricsReport?,
    onBack: () -> Unit
) {
    var tabIndex by remember { mutableStateOf(0) }
    val tabs = listOf("Config", "Reports")

    var filePath by remember { mutableStateOf("/data/local/tmp/config.pbin") }
    val activeConfigs = remember { mutableStateListOf<String>() }
    val inactiveConfigs = remember { mutableStateListOf<String>() }
    var selectedActiveConfig by remember { mutableStateOf<String?>(null) }
    var selectedInactiveConfig by remember { mutableStateOf<String?>(null) }

    Scaffold(
        topBar = {
            TopAppBar(title = { Text("Telemetry Sample Client") })
        }
    ) { padding ->
        Column(modifier = Modifier.padding(padding)) {
            PrimaryTabRow(selectedTabIndex = tabIndex) {
                tabs.forEachIndexed { index, title ->
                    Tab(
                        text = { Text(title) },
                        selected = tabIndex == index,
                        onClick = { tabIndex = index }
                    )
                }
            }
            when (tabIndex) {
                0 -> ConfigTab(
                    filePath = filePath,
                    onFilePathChange = { filePath = it },
                    activeConfigs = activeConfigs,
                    inactiveConfigs = inactiveConfigs,
                    selectedActiveConfig = selectedActiveConfig,
                    onSelectedActiveConfigChange = { selectedActiveConfig = it },
                    selectedInactiveConfig = selectedInactiveConfig,
                    onSelectedInactiveConfigChange = { selectedInactiveConfig = it },
                    onLoadConfig = onLoadConfig,
                    onGetConfigs = {
                        activeConfigs.clear()
                        inactiveConfigs.clear()
                        onGetConfigs(activeConfigs, inactiveConfigs)
                    },
                    onActivateConfig = onActivateConfig,
                    onDeactivateConfig = onDeactivateConfig
                )
                1 -> ReportsTab(
                    reports = reports,
                    onGetReport = onGetReport,
                    detailedReport = detailedReport,
                    onBack = onBack
                )
            }
        }
    }
}
