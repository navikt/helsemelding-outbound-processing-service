package no.nav.helsemelding.outbound.processing.stream

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext
import org.apache.kafka.streams.processor.api.FixedKeyRecord
import kotlin.uuid.Uuid

class OutboundMessageProcessorSpec : StringSpec(
    {
        "should validate and forward processed message" {
            val key = Uuid.random().toString()
            val payload = """{"hello":"world"}"""
            val headers = RecordHeaders()
                .add("sourceSystem", "some-system".encodeToByteArray())

            val record = mockk<FixedKeyRecord<String, String>> {
                every { key() } returns key
                every { value() } returns payload
                every { timestamp() } returns 123456789L
                every { headers() } returns headers
                every { withValue(any<ProcessedMessage>()) } answers {
                    mockk<FixedKeyRecord<String, ProcessedMessage>> {
                        every { value() } returns firstArg()
                    }
                }
            }

            val context = mockk<FixedKeyProcessorContext<String, ProcessedMessage>>(relaxed = true)

            OutboundMessageProcessor(OutboundMessageValidator()).apply {
                init(context)
                process(record)
            }

            val forwarded = slot<FixedKeyRecord<String, ProcessedMessage>>()

            verify(exactly = 1) {
                context.forward(capture(forwarded))
            }

            forwarded.captured.value().apply {
                this.key shouldBe key
                this.payload shouldBe payload
                this.validation.isValid() shouldBe true
            }
        }
    }
)
