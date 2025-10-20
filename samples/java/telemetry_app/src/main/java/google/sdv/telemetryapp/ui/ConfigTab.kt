// Copyright 2025 Google LLC

package google.sdv.telemetryapp.ui

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp

@Composable
fun ConfigTab(
    filePath: String,
    onFilePathChange: (String) -> Unit,
    activeConfigs: MutableList<String>,
    inactiveConfigs: MutableList<String>,
    selectedActiveConfig: String?,
    onSelectedActiveConfigChange: (String?) -> Unit,
    selectedInactiveConfig: String?,
    onSelectedInactiveConfigChange: (String?) -> Unit,
    onLoadConfig: (String) -> Unit,
    onGetConfigs: () -> Unit,
    onActivateConfig: (String) -> Unit,
    onDeactivateConfig: (String) -> Unit
) {
    Column(
        modifier = Modifier.fillMaxSize(),
        horizontalAlignment = Alignment.CenterHorizontally
    ) {
        OutlinedTextField(
            value = filePath,
            onValueChange = onFilePathChange,
            label = { Text("Enter file path") },
            modifier = Modifier
                .fillMaxWidth()
                .padding(16.dp)
        )
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.SpaceEvenly
        ) {
            Button(onClick = { onLoadConfig(filePath) }) {
                Text("Load Config")
            }
            Button(onClick = onGetConfigs) {
                Text("Get Configs")
            }
        }
        Row(modifier = Modifier.fillMaxWidth(), verticalAlignment = Alignment.CenterVertically) {
            Column(modifier = Modifier.weight(1f)) {
                Text("Active Configs", modifier = Modifier.padding(16.dp))
                LazyColumn {
                    items(activeConfigs) { config ->
                        Text(
                            text = config,
                            modifier = Modifier
                                .fillMaxWidth()
                                .clickable { onSelectedActiveConfigChange(config) }
                                .padding(8.dp)
                                .background(
                                    if (config == selectedActiveConfig) MaterialTheme.colorScheme.primaryContainer
                                    else Color.Transparent
                                )
                        )
                    }
                }
            }
            Column(
                horizontalAlignment = Alignment.CenterHorizontally,
                verticalArrangement = Arrangement.Center,
                modifier = Modifier.padding(8.dp)
            ) {
                Button(
                    onClick = {
                        selectedActiveConfig?.let {
                            onDeactivateConfig(it)
                            if (!inactiveConfigs.contains(it)) {
                                inactiveConfigs.add(it)
                            }
                            activeConfigs.remove(it)
                            onSelectedActiveConfigChange(null)
                        }
                    },
                    enabled = selectedActiveConfig != null
                ) {
                    Text(">")
                }
                Spacer(modifier = Modifier.height(16.dp))
                Button(
                    onClick = {
                        selectedInactiveConfig?.let {
                            onActivateConfig(it)
                            if (!activeConfigs.contains(it)) {
                                activeConfigs.add(it)
                            }
                            inactiveConfigs.remove(it)
                            onSelectedInactiveConfigChange(null)
                        }
                    },
                    enabled = selectedInactiveConfig != null
                ) {
                    Text("<")
                }
            }
            Column(modifier = Modifier.weight(1f)) {
                Text("Inactive Configs", modifier = Modifier.padding(16.dp))
                LazyColumn {
                    items(inactiveConfigs) { config ->
                        Text(
                            text = config,
                            modifier = Modifier
                                .fillMaxWidth()
                                .clickable { onSelectedInactiveConfigChange(config) }
                                .padding(8.dp)
                                .background(
                                    if (config == selectedInactiveConfig) MaterialTheme.colorScheme.primaryContainer
                                    else Color.Transparent
                                )
                        )
                    }
                }
            }
        }
    }
}
