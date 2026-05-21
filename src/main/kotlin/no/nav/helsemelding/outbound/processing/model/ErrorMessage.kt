package no.nav.helsemelding.outbound.processing.model

import kotlinx.serialization.Serializable
import kotlin.time.Instant

@Serializable
data class ErrorMessage(
    val processedAt: Instant,
    val error: ProcessingError,
    val originalMessage: OriginalMessage
)
