package no.nav.helsemelding.outbound.processing.model

import kotlin.time.Instant

data class ReceivedMessage(
    val key: String?,
    val payload: String,
    val sourceSystem: String?,
    val createdAt: Instant,
    val topic: String,
    val partition: Int,
    val offset: Long
)
