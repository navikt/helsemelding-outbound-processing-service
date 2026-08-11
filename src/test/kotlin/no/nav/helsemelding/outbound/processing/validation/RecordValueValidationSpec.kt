package no.nav.helsemelding.outbound.processing.validation

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

        "should return valid when value is present" {
            validateRecordValue("<xml></xml>") shouldBe
                RecordValueValidation.Valid
        }
    }
)
