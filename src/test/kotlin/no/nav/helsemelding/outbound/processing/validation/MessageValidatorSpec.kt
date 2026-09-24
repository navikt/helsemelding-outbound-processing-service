package no.nav.helsemelding.outbound.processing.validation

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import no.nav.helsemelding.outbound.processing.model.ErrorCategory
import no.nav.helsemelding.outbound.processing.model.ErrorCode
import no.nav.helsemelding.outbound.processing.model.ProcessingError

class MessageValidatorSpec : StringSpec(
    {
        "should map validation failures to processing errors" {
            val validation = MessageValidationResult(
                recordValue = RecordValueValidation.Valid,
                recordMetadata = RecordMetadataValidation.Invalid(
                    "Kafka record header 'sourcesystem' is missing"
                ),
                message = MessageValidation.Valid
            )

            validation.errors() shouldBe listOf(
                ProcessingError(
                    category = ErrorCategory.VALIDATION,
                    code = ErrorCode.MISSING_SOURCE_SYSTEM_HEADER,
                    message = "Kafka record header 'sourcesystem' is missing"
                )
            )
        }

        "should validate message after record value is valid json" {
            val validation = MessageValidator().validate(
                value = """{"hello":"world"}""",
                sourceSystem = "some-system"
            )

            validation.isValid() shouldBe false
            validation.errors().map { it.code } shouldBe listOf(ErrorCode.INVALID_MESSAGE)
        }

        "should be valid when all validation results are valid" {
            val validation = MessageValidationResult(
                recordValue = RecordValueValidation.Valid,
                recordMetadata = RecordMetadataValidation.Valid,
                message = MessageValidation.Valid
            )

            validation.isValid() shouldBe true
        }
    }
)
