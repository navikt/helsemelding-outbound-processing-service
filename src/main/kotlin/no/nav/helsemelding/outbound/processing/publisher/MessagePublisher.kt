package no.nav.helsemelding.outbound.processing.publisher

import arrow.core.Either
import arrow.core.Either.Right
import io.github.nomisRev.kafka.publisher.KafkaPublisher
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.serialization.json.Json
import no.nav.helsemelding.outbound.processing.PublishError
import no.nav.helsemelding.outbound.processing.config.Kafka
import no.nav.helsemelding.outbound.processing.model.ErrorMessage
import no.nav.helsemelding.outbound.processing.model.ProcessedMessage
import no.nav.helsemelding.outbound.processing.util.toEither
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition

private val log = KotlinLogging.logger {}

interface MessagePublisher {
    suspend fun publish(errorMessage: ErrorMessage): Either<PublishError, RecordMetadata>

    suspend fun publish(processedMessage: ProcessedMessage): Either<PublishError, RecordMetadata>
}

class OutboundMessagePublisher(
    private val topics: Kafka.Topics,
    private val kafkaPublisher: KafkaPublisher<String, ByteArray>,
    private val json: Json = Json
) : MessagePublisher {
    override suspend fun publish(errorMessage: ErrorMessage): Either<PublishError, RecordMetadata> =
        publish(
            topic = topics.errorMessage,
            key = errorMessage.originalMessage.key,
            payload = json.encodeToString(errorMessage)
        )

    override suspend fun publish(processedMessage: ProcessedMessage): Either<PublishError, RecordMetadata> =
        publish(
            topic = topics.dialogMessageOutXml,
            key = processedMessage.key,
            payload = processedMessage.payload
        )

    private suspend fun publish(
        topic: String,
        key: String,
        payload: String
    ): Either<PublishError, RecordMetadata> =
        kafkaPublisher.publishScope {
            publishCatching(
                ProducerRecord(
                    topic,
                    key,
                    payload.encodeToByteArray()
                )
            )
        }
            .toEither { error -> PublishError.Failure(key, topic, error) }
            .also { result ->
                when (result) {
                    is Right -> result.value.logPublished(topic, key)

                    else -> {}
                }
            }
}

private fun RecordMetadata.logPublished(topic: String, key: String) {
    log.info {
        "Published message key=$key topic=$topic partition=${partition()} offset=${offset()}"
    }
}

class FakeMessagePublisher(
    private val publishErrorMessageResult: Either<PublishError, RecordMetadata> = Either.Right(recordMetadata()),
    private val publishProcessedMessageResult: Either<PublishError, RecordMetadata> = Either.Right(recordMetadata())
) : MessagePublisher {
    val errorMessages = mutableListOf<ErrorMessage>()
    val processedMessages = mutableListOf<ProcessedMessage>()

    override suspend fun publish(errorMessage: ErrorMessage): Either<PublishError, RecordMetadata> {
        errorMessages.add(errorMessage)
        return publishErrorMessageResult
    }

    override suspend fun publish(processedMessage: ProcessedMessage): Either<PublishError, RecordMetadata> {
        processedMessages.add(processedMessage)
        return publishProcessedMessageResult
    }
}

private fun recordMetadata(): RecordMetadata =
    RecordMetadata(
        TopicPartition("topic", 0),
        0,
        0,
        0,
        0,
        0
    )
