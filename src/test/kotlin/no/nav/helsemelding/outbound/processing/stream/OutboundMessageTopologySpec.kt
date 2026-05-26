package no.nav.helsemelding.outbound.processing.stream

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import no.nav.helsemelding.outbound.processing.config
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.TestRecord

class OutboundMessageTopologySpec : StringSpec(
    {
        val kafkaStreams = config().kafkaStreamsSettings

        "should route invalid message to error topic" {
            val testDriver = TopologyTestDriver(
                OutboundMessageTopology(OutboundMessageValidator()).build(),
                kafkaStreams.toProperties()
            )

            testDriver.use { driver ->
                val inputTopic = driver.createInputTopic(
                    kafkaStreams.topics.dialogMessageIn,
                    Serdes.String().serializer(),
                    Serdes.String().serializer()
                )

                val outboundTopic = driver.createOutputTopic(
                    kafkaStreams.topics.dialogMessageOut,
                    Serdes.String().deserializer(),
                    Serdes.String().deserializer()
                )

                val errorTopic = driver.createOutputTopic(
                    kafkaStreams.topics.dialogMessageError,
                    Serdes.String().deserializer(),
                    Serdes.String().deserializer()
                )

                val key = "not-a-uuid"
                val payload = "<xml></xml>"
                val headers = RecordHeaders()

                inputTopic.pipeInput(
                    TestRecord(
                        key,
                        payload,
                        headers
                    )
                )

                outboundTopic.isEmpty shouldBe true

                val errors = errorTopic.readRecordsToList()

                errors.size shouldBe 1

                val errorRecord = errors.single()

                errorRecord.key() shouldBe key

                errorRecord.value().also { json ->
                    json shouldContain "INVALID_KAFKA_KEY"
                    json shouldContain "INVALID_KAFKA_VALUE"
                    json shouldContain "MISSING_SOURCE_SYSTEM_HEADER"
                }
            }
        }
    }
)
