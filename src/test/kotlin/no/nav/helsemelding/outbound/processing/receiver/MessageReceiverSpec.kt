package no.nav.helsemelding.outbound.processing.receiver

import app.cash.turbine.test
import app.cash.turbine.turbineScope
import arrow.fx.coroutines.resourceScope
import io.github.nomisRev.kafka.publisher.KafkaPublisher
import io.github.nomisRev.kafka.receiver.AutoOffsetReset
import io.kotest.matchers.shouldBe
import no.nav.helsemelding.outbound.processing.KafkaSpec
import no.nav.helsemelding.outbound.processing.config
import no.nav.helsemelding.outbound.processing.config.Config
import no.nav.helsemelding.outbound.processing.config.Kafka
import no.nav.helsemelding.outbound.processing.config.withKafka
import no.nav.helsemelding.outbound.processing.kafkaReceiver
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import kotlin.uuid.Uuid

class MessageReceiverSpec : KafkaSpec(
    {
        lateinit var config: Config

        beforeSpec {
            config = config()
                .withKafka {
                    copy(
                        bootstrapServers = container.bootstrapServers,
                        securityProtocol = Kafka.SecurityProtocol("PLAINTEXT")
                    )
                }
        }

        "should receive message with key, payload, source system and metadata" {
            resourceScope {
                turbineScope {
                    val publisher = install({ KafkaPublisher(publisherSettings()) }) { p, _ -> p.close() }
                    val key = Uuid.random()
                    val content = "data"
                    val sourceSystem = "source-system"
                    val topic = testTopic()
                    publisher.publishScope {
                        publish(
                            ProducerRecord(
                                topic,
                                key.toString(),
                                content.encodeToByteArray()
                            )
                                .withSourceSystem(sourceSystem)
                        )
                    }

                    val receiver = OutboundMessageReceiver(
                        topic,
                        kafkaReceiver(config.kafka, AutoOffsetReset.Earliest)
                    )
                    val messages = receiver.receiveMessages()

                    messages.test {
                        val message = awaitItem()
                        message.key shouldBe key.toString()
                        message.payload shouldBe content
                        message.sourceSystem shouldBe sourceSystem
                        message.topic shouldBe topic
                        message.partition shouldBe 0
                        cancelAndIgnoreRemainingEvents()
                    }
                }
            }
        }

        "should receive message without source system header" {
            resourceScope {
                turbineScope {
                    val publisher = install({ KafkaPublisher(publisherSettings()) }) { p, _ -> p.close() }
                    val key = Uuid.random()
                    val content = "data"
                    val topic = testTopic()
                    publisher.publishScope {
                        publish(
                            ProducerRecord(
                                topic,
                                key.toString(),
                                content.encodeToByteArray()
                            )
                        )
                    }

                    val receiver = OutboundMessageReceiver(
                        topic,
                        kafkaReceiver(config.kafka, AutoOffsetReset.Earliest)
                    )

                    receiver.receiveMessages().test {
                        val message = awaitItem()
                        message.key shouldBe key.toString()
                        message.payload shouldBe content
                        message.sourceSystem shouldBe null
                        message.topic shouldBe topic
                        cancelAndIgnoreRemainingEvents()
                    }
                }
            }
        }

        "should receive message without key" {
            resourceScope {
                turbineScope {
                    val publisher = install({ KafkaPublisher(publisherSettings()) }) { p, _ -> p.close() }
                    val content = "data"
                    val topic = testTopic()
                    publisher.publishScope {
                        publish(
                            ProducerRecord<String, ByteArray>(
                                topic,
                                null,
                                content.encodeToByteArray()
                            )
                        )
                    }

                    val receiver = OutboundMessageReceiver(
                        topic,
                        kafkaReceiver(config.kafka, AutoOffsetReset.Earliest)
                    )

                    receiver.receiveMessages().test {
                        val message = awaitItem()
                        message.key shouldBe null
                        message.payload shouldBe content
                        message.topic shouldBe topic
                        message.partition shouldBe 0
                        cancelAndIgnoreRemainingEvents()
                    }
                }
            }
        }
    }
)

private fun testTopic(): String = "test-dialog-message-out-json-${Uuid.random()}"

private fun <K, V> ProducerRecord<K, V>.withSourceSystem(sourceSystem: String): ProducerRecord<K, V> =
    apply {
        headers().add(
            RecordHeader(
                "sourceSystem",
                sourceSystem.encodeToByteArray()
            )
        )
    }
