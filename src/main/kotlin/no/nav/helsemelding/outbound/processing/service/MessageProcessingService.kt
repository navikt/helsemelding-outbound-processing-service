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
import no.nav.helsemelding.messageconverter.json.OutgoingDialogMessageSerializer
import no.nav.helsemelding.outbound.processing.PublishError
import no.nav.helsemelding.outbound.processing.conversion.OutgoingMessageConverter
import no.nav.helsemelding.outbound.processing.conversion.OutgoingMessageError
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
import no.nav.helsemelding.payloadsigning.client.PayloadSigningClient
import no.nav.helsemelding.payloadsigning.model.Direction.OUT
import no.nav.helsemelding.payloadsigning.model.MessageSigningError
import no.nav.helsemelding.payloadsigning.model.PayloadRequest
import org.apache.kafka.clients.producer.RecordMetadata
import kotlin.time.Clock

private val log = KotlinLogging.logger {}

class MessageProcessingService(
    private val messageReceiver: MessageReceiver,
    private val messagePublisher: MessagePublisher,
    private val messageValidator: MessageValidator,
    private val outgoingMessageConverter: OutgoingMessageConverter,
    private val payloadSigningClient: PayloadSigningClient,
    private val outgoingDialogMessageSerializer: OutgoingDialogMessageSerializer = OutgoingDialogMessageSerializer()
) {
    fun processMessages(scope: CoroutineScope): Job =
        messageReceiver
            .receiveMessages()
            .onEach(::processMessage)
            .launchIn(scope)

    internal suspend fun processMessage(message: ReceivedMessage) {
        message.logReceived()

        when (val result = message.validate()) {
            is Left -> result.logPublishError()
            is Right -> message.acknowledge()
        }
    }

    private suspend fun ReceivedMessage.validate(): Either<PublishError, RecordMetadata> {
        val validationResult = messageValidator.validate(
            value = payload,
            sourceSystem = sourceSystem
        )

        return when (validationResult.isValid()) {
            true -> extractMessageId()
            false -> publishErrorMessage(validationResult)
        }
    }

    private suspend fun ReceivedMessage.extractMessageId(): Either<PublishError, RecordMetadata> =
        when (val result = outgoingDialogMessageSerializer.deserialize(payload)) {
            is Left ->
                messagePublisher.publish(
                    toErrorMessage(
                        listOf(
                            result.value.toProcessingError()
                        )
                    )
                )
            is Right -> {
                val messageWithId = this.copy(messageId = result.value.id)
                messageWithId.convertToXml()
            }
        }

    private suspend fun ReceivedMessage.publishErrorMessage(
        validation: MessageValidationResult
    ): Either<PublishError, RecordMetadata> =
        messagePublisher.publish(
            toErrorMessage(validation.errors())
        )

    private suspend fun ReceivedMessage.convertToXml(): Either<PublishError, RecordMetadata> =
        when (val result = outgoingMessageConverter.outgoingDialogMessageJsonToXml(payload)) {
            is Left ->
                messagePublisher.publish(
                    toErrorMessage(
                        listOf(
                            result.value.toProcessingError()
                        )
                    )
                )
            is Right -> sign(result.value)
        }

    private suspend fun ReceivedMessage.sign(xml: String): Either<PublishError, RecordMetadata> =
        when (val result = payloadSigningClient.signPayload(PayloadRequest(OUT, xml.encodeToByteArray()))) {
            is Left ->
                messagePublisher.publish(
                    toErrorMessage(
                        listOf(
                            result.value.toProcessingError()
                        )
                    )
                )

            is Right -> {
                val signedMessage = result.value.bytes.decodeToString()
                messagePublisher.publish(toProcessedMessage(signedMessage))
            }
        }
}

private fun ReceivedMessage.logReceived() {
    log.info {
        "Received message: topic=$topic partition=$partition offset=$offset"
    }
}

private fun ReceivedMessage.toProcessedMessage(xmlPayload: String): ProcessedMessage =
    ProcessedMessage(
        key = messageId.toString(),
        payload = xmlPayload
    )

private fun ReceivedMessage.toErrorMessage(errors: List<ProcessingError>): ErrorMessage =
    ErrorMessage(
        processedAt = Clock.System.now(),
        sourceSystem = sourceSystem ?: "UNKNOWN",
        errors = errors,
        originalMessage = OriginalMessage(
            createdAt = createdAt,
            key = messageId.toString(),
            payload = payload
        )
    )

private fun OutgoingMessageError.toProcessingError(): ProcessingError =
    ProcessingError(
        category = ErrorCategory.CONVERSION,
        code = code,
        message = message
    )

private fun ConversionError.toProcessingError(): ProcessingError =
    ProcessingError(
        category = ErrorCategory.CONVERSION,
        code = ErrorCode.MESSAGE_ID_EXTRACTION_ERROR,
        message = message
    )

private fun MessageSigningError.toProcessingError(): ProcessingError =
    ProcessingError(
        category = ErrorCategory.SIGNING,
        code = ErrorCode.SIGNING_ERROR,
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
