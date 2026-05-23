package no.nav.helsemelding.outbound.processing.stream

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import org.apache.kafka.common.header.internals.RecordHeaders

class RecordMetadataValidationSpec : StringSpec(
    {
        "should return invalid when sourceSystem header is missing" {
            val headers = RecordHeaders()

            validateRecordMetadata(headers) shouldBe RecordMetadataValidation.Invalid(
                "Kafka record header 'sourceSystem' is missing or empty"
            )
        }

        "should return invalid when sourceSystem header is empty" {
            val headers = RecordHeaders()
                .add("sourceSystem", "".encodeToByteArray())

            validateRecordMetadata(headers) shouldBe RecordMetadataValidation.Invalid(
                "Kafka record header 'sourceSystem' is missing or empty"
            )
        }

        "should return invalid when sourceSystem header is blank" {
            val headers = RecordHeaders()
                .add("sourceSystem", "   ".encodeToByteArray())

            validateRecordMetadata(headers) shouldBe RecordMetadataValidation.Invalid(
                "Kafka record header 'sourceSystem' is missing or empty"
            )
        }

        "should return valid when sourceSystem header exists" {
            val headers = RecordHeaders()
                .add("sourceSystem", "some-system".encodeToByteArray())

            validateRecordMetadata(headers) shouldBe RecordMetadataValidation.Valid
        }
    }
)
