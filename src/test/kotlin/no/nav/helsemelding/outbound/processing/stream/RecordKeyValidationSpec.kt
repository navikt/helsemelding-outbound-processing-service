package no.nav.helsemelding.outbound.processing.stream

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlin.uuid.Uuid

class RecordKeyValidationSpec : StringSpec(
    {
        "should return invalid when key is null" {
            validateRecordKey(null) shouldBe RecordKeyValidation.Invalid(
                "Kafka record key is null"
            )
        }

        "should return invalid when key is not uuid" {
            validateRecordKey("not-a-uuid") shouldBe RecordKeyValidation.Invalid(
                "Kafka record key is not a valid UUID"
            )
        }

        "should return valid when key is uuid" {
            validateRecordKey(Uuid.random().toString()) shouldBe RecordKeyValidation.Valid
        }
    }
)
