package no.nav.helsemelding.outbound.processing.stream

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import org.apache.kafka.common.header.internals.RecordHeaders

class RecordMetadataValidationSpec : StringSpec(
    {
        "should return invalid when sourcesystem header is missing" {
            val headers = RecordHeaders()

            validateRecordMetadata(headers) shouldBe RecordMetadataValidation.Invalid(
                "Kafka record header 'sourcesystem' is missing"
            )
        }

        "should return valid when sourcesystem header exists" {
            val headers = RecordHeaders()
                .add("sourcesystem", "some-system".encodeToByteArray())

            validateRecordMetadata(headers) shouldBe RecordMetadataValidation.Valid
        }
    }
)
