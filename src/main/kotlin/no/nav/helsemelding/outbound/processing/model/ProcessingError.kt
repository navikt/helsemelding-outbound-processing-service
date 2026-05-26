package no.nav.helsemelding.outbound.processing.model

import kotlinx.serialization.Serializable

@Serializable
data class ProcessingError(
    val category: ErrorCategory,
    val code: ErrorCode,
    val message: String
)
