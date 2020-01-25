package org.ndp.port_scan_ns.bean

import com.squareup.moshi.Json

data class KafkaResult(
    @Json(name = "task-id") val taskID: Int,
    val result: List<Host>,
    val status: Int,
    val desc: String
)