package no.nav.helsemelding.outbound.processing.stream

import arrow.core.Either
import arrow.core.getOrElse
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import no.nav.helsemelding.outbound.processing.model.ErrorCategory
import no.nav.helsemelding.outbound.processing.model.ErrorCode
import no.nav.helsemelding.outbound.processing.model.ProcessingError
import org.apache.kafka.common.header.Headers
import kotlin.uuid.Uuid

data class OutboundMessageValidation(
    val recordKey: RecordKeyValidation,
    val recordValue: RecordValueValidation,
    val recordMetadata: RecordMetadataValidation
)

fun OutboundMessageValidation.isValid(): Boolean =
    recordKey.isValid &&
        recordValue.isValid &&
        recordMetadata.isValid

fun OutboundMessageValidation.errors(): List<ProcessingError> =
    buildList {
        when (val key = recordKey) {
            is RecordKeyValidation.Invalid ->
                add(
                    ProcessingError(
                        category = ErrorCategory.VALIDATION,
                        code = ErrorCode.INVALID_KAFKA_KEY,
                        message = key.reason
                    )
                )

            RecordKeyValidation.Valid -> Unit
        }

        when (val value = recordValue) {
            is RecordValueValidation.Invalid ->
                add(
                    ProcessingError(
                        category = ErrorCategory.VALIDATION,
                        code = ErrorCode.INVALID_KAFKA_VALUE,
                        message = value.reason
                    )
                )

            RecordValueValidation.Valid -> Unit
        }

        when (val metadata = recordMetadata) {
            is RecordMetadataValidation.Invalid ->
                add(
                    ProcessingError(
                        category = ErrorCategory.VALIDATION,
                        code = ErrorCode.MISSING_SOURCE_SYSTEM_HEADER,
                        message = metadata.reason
                    )
                )

            RecordMetadataValidation.Valid -> Unit
        }
    }

class OutboundMessageValidator {
    fun validate(
        key: String?,
        value: ByteArray?,
        headers: Headers
    ): OutboundMessageValidation =
        OutboundMessageValidation(
            recordKey = validateRecordKey(key),
            recordValue = validateRecordValue(value),
            recordMetadata = validateRecordMetadata(headers)
        )
}

sealed interface Validation {
    val isValid: Boolean
}

sealed interface RecordKeyValidation : Validation {
    data object Valid : RecordKeyValidation {
        override val isValid = true
    }

    data class Invalid(
        val reason: String
    ) : RecordKeyValidation {
        override val isValid = false
    }
}

internal fun validateRecordKey(
    key: String?
): RecordKeyValidation =
    when {
        key == null ->
            RecordKeyValidation.Invalid(
                "Kafka record key is null"
            )

        Uuid.parseOrNull(key) == null ->
            RecordKeyValidation.Invalid(
                "Kafka record key is not a valid UUID"
            )

        else -> RecordKeyValidation.Valid
    }

sealed interface RecordValueValidation : Validation {
    data object Valid : RecordValueValidation {
        override val isValid = true
    }

    data class Invalid(
        val reason: String
    ) : RecordValueValidation {
        override val isValid = false
    }
}

// NOTE: Should also validate the JSON schema
internal fun validateRecordValue(
    value: ByteArray?
): RecordValueValidation =
    when {
        value == null ->
            RecordValueValidation.Invalid(
                "Kafka record value is null"
            )

        value.isEmpty() ->
            RecordValueValidation.Invalid(
                "Kafka record value is empty"
            )

        !value.isValidJson() ->
            RecordValueValidation.Invalid(
                "Kafka record value is not valid JSON"
            )

        else -> RecordValueValidation.Valid
    }

private fun ByteArray.isValidJson(): Boolean =
    Either.catch {
        Json.parseToJsonElement(decodeToString()) is JsonObject
    }
        .getOrElse { false }

sealed interface RecordMetadataValidation : Validation {
    data object Valid : RecordMetadataValidation {
        override val isValid = true
    }

    data class Invalid(
        val reason: String
    ) : RecordMetadataValidation {
        override val isValid = false
    }
}

private const val SOURCE_SYSTEM_HEADER = "sourcesystem"

internal fun validateRecordMetadata(
    headers: Headers
): RecordMetadataValidation =
    when {
        headers.lastHeader(SOURCE_SYSTEM_HEADER) == null ->
            RecordMetadataValidation.Invalid(
                "Kafka record header '$SOURCE_SYSTEM_HEADER' is missing"
            )

        else -> RecordMetadataValidation.Valid
    }
