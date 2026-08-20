package no.nav.helsemelding.outbound.processing.model

enum class ErrorCode {
    INVALID_KAFKA_KEY,
    INVALID_KAFKA_VALUE,
    MISSING_SOURCE_SYSTEM_HEADER,
    INVALID_MESSAGE,
    CONVERSION_ERROR,
    PDL_ERROR,
    PROVIDER_REGISTRY_ERROR
}
