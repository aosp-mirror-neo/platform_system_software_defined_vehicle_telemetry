// Copyright 2025 Google LLC

package google.sdv.telemetryapp.ui

import google.sdv.telemetry.MetricsReport
import google.sdv.telemetry.MetricsReportInfo
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.runtime.snapshots.SnapshotStateMap
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp

@Composable
fun ReportsTab(
    reports: SnapshotStateMap<String, MetricsReportInfo>,
    onGetReport: (String) -> Unit,
    detailedReport: MetricsReport?,
    onBack: () -> Unit
) {
    if (detailedReport != null) {
        ReportDetailView(report = detailedReport, onBack = onBack)
    } else {
        LazyColumn(modifier = Modifier.fillMaxSize()) {
            items(reports.values.toList()) { reportInfo ->
                Row(
                    modifier = Modifier
                        .fillMaxWidth()
                        .padding(16.dp),
                    horizontalArrangement = Arrangement.SpaceBetween,
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    Column(modifier = Modifier.weight(1f)) {
                        Text("Config UUID: ${reportInfo.configUuid}")
                        Text("Report UUID: ${reportInfo.reportUuid}")
                    }
                    Button(onClick = { onGetReport(reportInfo.reportUuid) }) {
                        Text("Details")
                    }
                }
                HorizontalDivider()
            }
        }
    }
}
