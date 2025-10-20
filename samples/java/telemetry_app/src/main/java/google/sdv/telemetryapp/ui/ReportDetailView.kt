// Copyright 2025 Google LLC

package google.sdv.telemetryapp.ui

import google.sdv.telemetry.MetricsReport
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import java.time.Instant

@Composable
fun ReportDetailView(report: MetricsReport, onBack: () -> Unit) {
    Column(modifier = Modifier.padding(16.dp)) {
        Button(onClick = onBack) {
            Text("Back")
        }
        Spacer(modifier = Modifier.height(16.dp))
        Text("Report Details:", style = MaterialTheme.typography.headlineSmall)
        Spacer(modifier = Modifier.height(16.dp))
        LazyColumn(verticalArrangement = Arrangement.spacedBy(8.dp)) {
            item {
                Row {
                    Text("Report UUID: ", fontWeight = FontWeight.Bold)
                    Text(report.reportUuid)
                }
            }
            item {
                Row {
                    Text("Report Number: ", fontWeight = FontWeight.Bold)
                    Text(report.reportNumber.toString())
                }
            }
            item {
                Row {
                    Text("Report Reason: ", fontWeight = FontWeight.Bold)
                    Text(report.reportReason.toString())
                }
            }
            item {
                Row {
                    Text("Metrics Config UUID: ", fontWeight = FontWeight.Bold)
                    Text(report.metricsConfigUuid)
                }
            }
            item {
                Row {
                    Text("Metrics Config Version: ", fontWeight = FontWeight.Bold)
                    Text(report.metricsConfigVersion.toString())
                }
            }
            item {
                Row {
                    Text("Report Config Name: ", fontWeight = FontWeight.Bold)
                    Text(report.reportConfigName)
                }
            }
            item {
                Row {
                    Text("Report Timestamp: ", fontWeight = FontWeight.Bold)
                    Text(formatTimestamp(report.reportTimestamp))
                }
            }
            item {
                Row {
                    Text("Config Activation Timestamp: ", fontWeight = FontWeight.Bold)
                    Text(formatTimestamp(report.configActivationTimestamp))
                }
            }
            item {
                Row {
                    Text("Error IDs: ", fontWeight = FontWeight.Bold)
                    Text(report.errorIdsList.joinToString())
                }
            }
            item {
                Row {
                    Text("Report Data: ", fontWeight = FontWeight.Bold)
                    Text(report.reportData.toString())
                }
            }
        }
    }
}

private fun formatTimestamp(timestamp: com.google.protobuf.Timestamp): String {
    if (timestamp.seconds == 0L && timestamp.nanos == 0) {
        return "N/A"
    }
    val instant = Instant.ofEpochSecond(timestamp.seconds, timestamp.nanos.toLong())
    return instant.toString()
}
