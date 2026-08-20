package no.nav.helsemelding.outbound.processing.publisher

import app.cash.turbine.test
import arrow.fx.coroutines.resourceScope
import io.github.nomisRev.kafka.publisher.KafkaPublisher
import io.github.nomisRev.kafka.publisher.TransactionalScope
import io.github.nomisRev.kafka.receiver.KafkaReceiver
import io.kotest.assertions.arrow.core.shouldBeLeft
import io.kotest.assertions.arrow.core.shouldBeRight
import io.kotest.matchers.shouldBe
import kotlinx.serialization.json.Json
import no.nav.helsemelding.outbound.processing.KafkaSpec
import no.nav.helsemelding.outbound.processing.PublishError
import no.nav.helsemelding.outbound.processing.config.Kafka
import no.nav.helsemelding.outbound.processing.model.ErrorCategory
import no.nav.helsemelding.outbound.processing.model.ErrorCode
import no.nav.helsemelding.outbound.processing.model.ErrorMessage
import no.nav.helsemelding.outbound.processing.model.OriginalMessage
import no.nav.helsemelding.outbound.processing.model.ProcessedMessage
import no.nav.helsemelding.outbound.processing.model.ProcessingError
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.PartitionInfo
import kotlin.time.Clock
import kotlin.uuid.Uuid

class MessagePublisherSpec : KafkaSpec(
    {
        "should publish processed message to xml topic" {
            resourceScope {
                val topics = testTopics()
                val kafkaPublisher = install({ KafkaPublisher(publisherSettings()) }) { p, _ -> p.close() }
                val publisher = OutboundMessagePublisher(
                    topics = topics,
                    kafkaPublisher = kafkaPublisher
                )
                val key = Uuid.random().toString()
                val xmlPayload = "<xml />"

                publisher.publish(
                    ProcessedMessage(
                        key = key,
                        payload = xmlPayload
                    )
                )
                    .shouldBeRight()

                KafkaReceiver(receiverSettings())
                    .receive(topics.dialogMessageOutXml)
                    .test {
                        val record = awaitItem()
                        record.key() shouldBe key
                        record.value().decodeToString() shouldBe xmlPayload
                        record.topic() shouldBe topics.dialogMessageOutXml

                        cancelAndIgnoreRemainingEvents()
                    }
            }
        }

        "should publish error message to error topic" {
            resourceScope {
                val topics = testTopics()
                val kafkaPublisher = install({ KafkaPublisher(publisherSettings()) }) { p, _ -> p.close() }
                val publisher = OutboundMessagePublisher(
                    topics = topics,
                    kafkaPublisher = kafkaPublisher
                )
                val errorMessage = errorMessage()

                publisher.publish(errorMessage).shouldBeRight()

                val records = KafkaReceiver(receiverSettings()).receive(topics.errorMessage)
                records.test {
                    val record = awaitItem()
                    val payload = Json.decodeFromString<ErrorMessage>(record.value().decodeToString())

                    record.key() shouldBe errorMessage.originalMessage.key
                    record.topic() shouldBe topics.errorMessage
                    payload shouldBe errorMessage

                    cancelAndIgnoreRemainingEvents()
                }
            }
        }

        "should return publish error when publishing fails" {
            val topics = testTopics()
            val failure = RuntimeException("Kafka publish failed")
            val publisher = OutboundMessagePublisher(
                topics = topics,
                kafkaPublisher = FailingKafkaPublisher(failure)
            )
            val processedMessage = ProcessedMessage(
                key = Uuid.random().toString(),
                payload = "<xml />"
            )

            val error = publisher.publish(processedMessage).shouldBeLeft()

            error shouldBe PublishError.Failure(
                key = processedMessage.key,
                topic = topics.dialogMessageOutXml,
                cause = failure
            )
        }
    }
)

private fun testTopics(): Kafka.Topics =
    Kafka.Topics(
        dialogMessageOutJson = "test-dialog-message-out-json-${Uuid.random()}",
        dialogMessageOutXml = "test-dialog-message-out-xml-${Uuid.random()}",
        errorMessage = "test-error-message-${Uuid.random()}"
    )

private fun errorMessage(): ErrorMessage =
    ErrorMessage(
        processedAt = Clock.System.now(),
        sourceSystem = "test-system",
        errors = listOf(
            ProcessingError(
                category = ErrorCategory.VALIDATION,
                code = ErrorCode.INVALID_KAFKA_KEY,
                message = "Invalid key"
            )
        ),
        originalMessage = OriginalMessage(
            createdAt = Clock.System.now(),
            key = Uuid.random().toString(),
            payload = "original payload"
        )
    )

private class FailingKafkaPublisher(
    private val failure: Throwable
) : KafkaPublisher<String, ByteArray> {
    @Suppress("UNCHECKED_CAST")
    override suspend fun <A> publishScope(block: suspend TransactionalScope<String, ByteArray>.() -> A): A =
        Result.failure<RecordMetadata>(failure) as A

    override suspend fun partitionsFor(topic: String): List<PartitionInfo> = emptyList()

    override suspend fun metrics(): Map<MetricName, Metric> = emptyMap()

    override fun close() = Unit
}
