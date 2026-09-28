package no.nav.helsemelding.outbound.processing.model

enum class ErrorCode {
    INVALID_KAFKA_VALUE,
    MISSING_SOURCE_SYSTEM_HEADER,
    INVALID_MESSAGE,
    CONVERSION_ERROR,
    MESSAGE_ID_EXTRACTION_ERROR,
    PDL_ERROR,
    PROVIDER_REGISTRY_ERROR,
    SIGNING_ERROR
}
