package no.nav.helsemelding.outbound.processing.stream

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe

class RecordValueValidationSpec : StringSpec(
    {
        "should return invalid when value is null" {
            validateRecordValue(null) shouldBe RecordValueValidation.Invalid(
                "Kafka record value is null"
            )
        }

        "should return invalid when value is empty" {
            validateRecordValue(String()) shouldBe RecordValueValidation.Invalid(
                "Kafka record value is empty"
            )
        }

        "should return invalid when value is not valid json" {
            validateRecordValue("<xml></xml>") shouldBe
                RecordValueValidation.Invalid("Kafka record value is not valid JSON")
        }

        "should return valid when value is valid json" {
            validateRecordValue("""{"hello":"world"}""") shouldBe
                RecordValueValidation.Valid
        }
    }
)
