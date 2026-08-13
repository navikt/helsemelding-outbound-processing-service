package no.nav.helsemelding.outbound.processing.validation

import arrow.core.Either
import arrow.core.Either.Left
import arrow.core.Either.Right
import no.nav.helsemelding.jsonschema.core.model.SchemaType
import no.nav.helsemelding.jsonschema.core.validation.JsonSchemaValidator
import no.nav.helsemelding.jsonschema.core.validation.SchemaValidator
import no.nav.helsemelding.jsonschema.core.validation.ValidationError
import no.nav.helsemelding.outbound.processing.model.ErrorCategory
import no.nav.helsemelding.outbound.processing.model.ErrorCode
import no.nav.helsemelding.outbound.processing.model.ProcessingError
import kotlin.uuid.Uuid

private const val SOURCE_SYSTEM_HEADER = "sourceSystem"

data class MessageValidationResult(
    val recordKey: RecordKeyValidation,
    val recordValue: RecordValueValidation,
    val recordMetadata: RecordMetadataValidation,
    val message: MessageValidation
)

fun MessageValidationResult.isValid(): Boolean =
    recordKey.isValid &&
        recordValue.isValid &&
        recordMetadata.isValid &&
        message.isValid

fun MessageValidationResult.errors(): List<ProcessingError> =
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

        when (val message = message) {
            is MessageValidation.Invalid ->
                add(
                    ProcessingError(
                        category = ErrorCategory.VALIDATION,
                        code = ErrorCode.INVALID_MESSAGE,
                        message = message.reason
                    )
                )

            MessageValidation.Valid -> Unit
        }
    }

class MessageValidator(
    private val schemaValidator: SchemaValidator = JsonSchemaValidator()
) {
    fun validate(
        key: String?,
        value: String?,
        sourceSystem: String?
    ): MessageValidationResult {
        val recordValue = validateRecordValue(value)

        return MessageValidationResult(
            recordKey = validateRecordKey(key),
            recordValue = recordValue,
            recordMetadata = validateRecordMetadata(sourceSystem),
            message = validateMessage(value, recordValue)
        )
    }

    private fun validateMessage(
        value: String?,
        recordValue: RecordValueValidation
    ): MessageValidation = when (recordValue) {
        RecordValueValidation.Valid -> validateMessage(value.orEmpty(), schemaValidator)
        is RecordValueValidation.Invalid -> MessageValidation.Valid
    }
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

internal fun validateRecordValue(
    value: String?
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

        else -> RecordValueValidation.Valid
    }

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

internal fun validateRecordMetadata(sourceSystem: String?): RecordMetadataValidation {
    return when {
        sourceSystem.isNullOrBlank() ->
            RecordMetadataValidation.Invalid(
                "Kafka record header '$SOURCE_SYSTEM_HEADER' is missing or empty"
            )

        else -> RecordMetadataValidation.Valid
    }
}

sealed interface MessageValidation : Validation {
    data object Valid : MessageValidation {
        override val isValid = true
    }

    data class Invalid(
        val reason: String
    ) : MessageValidation {
        override val isValid = false
    }
}

internal fun validateMessage(
    value: String,
    schemaValidator: SchemaValidator = JsonSchemaValidator()
): MessageValidation {
    return when (
        val result = schemaValidator.validate(
            schemaType = SchemaType.OUTGOING_DIALOG_MESSAGE,
            json = value
        )
    ) {
        is Left -> MessageValidation.Invalid(
            "Kafka record value is not a valid outgoing dialog message: ${result.value.errors.joinToString()}"
        )

        is Right -> MessageValidation.Valid
    }
}

class FakeSchemaValidator : SchemaValidator {
    val payloads = mutableListOf<String>()

    override fun validate(schemaType: SchemaType, json: String): Either<ValidationError, String> {
        payloads.add(json)
        return Right(json)
    }
}
