package no.nav.helsemelding.outbound.processing.model

import kotlin.time.Instant
import kotlin.uuid.Uuid

data class ReceivedMessage(
    val messageId: Uuid? = null,
    val key: String?,
    val payload: String,
    val sourceSystem: String?,
    val createdAt: Instant,
    val topic: String,
    val partition: Int,
    val offset: Long,
    val acknowledge: suspend () -> Unit = {}
)
