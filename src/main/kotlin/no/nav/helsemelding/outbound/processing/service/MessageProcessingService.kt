package no.nav.helsemelding.outbound.processing.service

import arrow.core.Either
import arrow.core.Either.Left
import arrow.core.Either.Right
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import no.nav.helsemelding.messageconverter.error.ConversionError
import no.nav.helsemelding.outbound.processing.PublishError
import no.nav.helsemelding.outbound.processing.conversion.OutgoingMessageConverter
import no.nav.helsemelding.outbound.processing.model.ErrorCategory
import no.nav.helsemelding.outbound.processing.model.ErrorCode
import no.nav.helsemelding.outbound.processing.model.ErrorMessage
import no.nav.helsemelding.outbound.processing.model.OriginalMessage
import no.nav.helsemelding.outbound.processing.model.ProcessedMessage
import no.nav.helsemelding.outbound.processing.model.ProcessingError
import no.nav.helsemelding.outbound.processing.model.ReceivedMessage
import no.nav.helsemelding.outbound.processing.publisher.MessagePublisher
import no.nav.helsemelding.outbound.processing.receiver.MessageReceiver
import no.nav.helsemelding.outbound.processing.validation.MessageValidationResult
import no.nav.helsemelding.outbound.processing.validation.MessageValidator
import no.nav.helsemelding.outbound.processing.validation.errors
import no.nav.helsemelding.outbound.processing.validation.isValid
import org.apache.kafka.clients.producer.RecordMetadata
import kotlin.time.Clock

private val log = KotlinLogging.logger {}

class MessageProcessingService(
    private val messageReceiver: MessageReceiver,
    private val messagePublisher: MessagePublisher,
    private val messageValidator: MessageValidator,
    private val outgoingMessageConverter: OutgoingMessageConverter
) {
    fun processMessages(scope: CoroutineScope): Job =
        messageReceiver
            .receiveMessages()
            .onEach(::processMessage)
            .launchIn(scope)

    internal suspend fun processMessage(message: ReceivedMessage) {
        message.logReceived()

        val validation = message.validate()

        when (val result = message.publish(validation)) {
            is Left -> result.logPublishError()
            is Right -> message.acknowledge()
        }
    }

    private fun ReceivedMessage.validate(): MessageValidationResult =
        messageValidator.validate(
            key = key,
            value = payload,
            sourceSystem = sourceSystem
        )

    private suspend fun ReceivedMessage.publish(
        validation: MessageValidationResult
    ): Either<PublishError, RecordMetadata> =
        when (validation.isValid()) {
            true -> publishMessage()
            false -> publishErrorMessage(validation)
        }

    private suspend fun ReceivedMessage.publishErrorMessage(
        validation: MessageValidationResult
    ): Either<PublishError, RecordMetadata> =
        messagePublisher.publish(
            toErrorMessage(validation.errors())
        )

    private suspend fun ReceivedMessage.publishMessage(): Either<PublishError, RecordMetadata> =
        when (val result = outgoingMessageConverter.outgoingDialogMessageJsonToXml(payload)) {
            is Left ->
                messagePublisher.publish(
                    toErrorMessage(
                        listOf(
                            result.value.toProcessingError()
                        )
                    )
                )

            is Right -> messagePublisher.publish(toProcessedMessage(result.value))
        }
}

private fun ReceivedMessage.logReceived() {
    log.info {
        "Received message: key=$key topic=$topic partition=$partition offset=$offset"
    }
}

private fun ReceivedMessage.validKey(): String =
    requireNotNull(key) { "Message key must be present after validation" }

private fun ReceivedMessage.toProcessedMessage(xmlPayload: String): ProcessedMessage =
    ProcessedMessage(
        key = validKey(),
        payload = xmlPayload
    )

private fun ReceivedMessage.toErrorMessage(errors: List<ProcessingError>): ErrorMessage =
    ErrorMessage(
        processedAt = Clock.System.now(),
        sourceSystem = sourceSystem ?: "UNKNOWN",
        errors = errors,
        originalMessage = OriginalMessage(
            createdAt = createdAt,
            key = key.orEmpty(),
            payload = payload
        )
    )

private fun ConversionError.toProcessingError(): ProcessingError =
    ProcessingError(
        category = ErrorCategory.CONVERSION,
        code = ErrorCode.CONVERSION_ERROR,
        message = message
    )

private fun Either<PublishError, RecordMetadata>.logPublishError() {
    when (this) {
        is Left -> log.error(value.cause) {
            "Failed to publish message key=${value.key} topic=${value.topic}"
        }

        is Right -> Unit
    }
}
