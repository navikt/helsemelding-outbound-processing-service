package no.nav.helsemelding.outbound.processing.stream

import io.kotest.core.spec.style.StringSpec
import io.kotest.datatest.withData
import io.kotest.matchers.shouldBe

class RecordMetadataValidationSpec : StringSpec(
    {
        withData(
            mapOf(
                "missing" to null,
                "empty" to "",
                "blank" to "    "
            )
        ) { sourceSystem ->
            validateRecordMetadata(sourceSystem) shouldBe
                RecordMetadataValidation.Invalid(
                    "Kafka record header 'sourceSystem' is missing or empty"
                )
        }

        "should return valid when sourceSystem header exists" {
            validateRecordMetadata("some-system") shouldBe
                RecordMetadataValidation.Valid
        }
    }
)
