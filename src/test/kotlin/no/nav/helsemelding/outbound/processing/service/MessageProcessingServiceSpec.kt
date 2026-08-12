package no.nav.helsemelding.outbound.processing.service

import arrow.core.Either
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import no.nav.helsemelding.jsonschema.core.validation.SchemaValidator
import no.nav.helsemelding.messageconverter.error.AdditionalMessageInfoError
import no.nav.helsemelding.outbound.processing.PublishError
import no.nav.helsemelding.outbound.processing.conversion.FakeOutgoingMessageConverter
import no.nav.helsemelding.outbound.processing.conversion.OutgoingMessageConverter
import no.nav.helsemelding.outbound.processing.model.ErrorCategory
import no.nav.helsemelding.outbound.processing.model.ErrorCode
import no.nav.helsemelding.outbound.processing.model.ProcessedMessage
import no.nav.helsemelding.outbound.processing.model.ProcessingError
import no.nav.helsemelding.outbound.processing.model.ReceivedMessage
import no.nav.helsemelding.outbound.processing.publisher.FakeMessagePublisher
import no.nav.helsemelding.outbound.processing.publisher.MessagePublisher
import no.nav.helsemelding.outbound.processing.receiver.FakeMessageReceiver
import no.nav.helsemelding.outbound.processing.receiver.MessageReceiver
import no.nav.helsemelding.outbound.processing.validation.FakeSchemaValidator
import no.nav.helsemelding.outbound.processing.validation.MessageValidator
import kotlin.time.Clock
import kotlin.uuid.Uuid

class MessageProcessingServiceSpec : StringSpec(
    {
        "should publish validation error when received message is invalid" {
            val acknowledgement = Acknowledgement()
            val message = receivedMessage(
                key = null,
                payload = "",
                sourceSystem = null,
                acknowledge = acknowledgement::acknowledge
            )
            val receiver = FakeMessageReceiver(message)
            val publisher = FakeMessagePublisher()
            val converter = FakeOutgoingMessageConverter()

            val service = messageProcessingService(
                receiver = receiver,
                publisher = publisher,
                converter = converter
            )

            service.processMessage(message)

            val errorMessage = publisher.errorMessages.single()
            errorMessage.sourceSystem shouldBe "UNKNOWN"
            errorMessage.originalMessage.key shouldBe ""
            errorMessage.originalMessage.payload shouldBe message.payload
            errorMessage.errors.map { it.code } shouldContainExactly listOf(
                ErrorCode.INVALID_KAFKA_KEY,
                ErrorCode.INVALID_KAFKA_VALUE,
                ErrorCode.MISSING_SOURCE_SYSTEM_HEADER
            )
            publisher.processedMessages shouldBe emptyList()
            converter.payloads shouldBe emptyList()
            acknowledgement.acknowledged shouldBe true
        }

        "should convert and publish processed message when received message is valid" {
            val key = Uuid.random().toString()
            val acknowledgement = Acknowledgement()
            val message = receivedMessage(
                key = key,
                acknowledge = acknowledgement::acknowledge
            )
            val receiver = FakeMessageReceiver(message)
            val publisher = FakeMessagePublisher()
            val converter = FakeOutgoingMessageConverter(Either.Right("<xml />"))

            val service = messageProcessingService(
                receiver = receiver,
                publisher = publisher,
                converter = converter,
                schemaValidator = FakeSchemaValidator()
            )

            service.processMessage(message)

            publisher.processedMessages.single() shouldBe ProcessedMessage(
                key = key,
                payload = "<xml />"
            )
            publisher.errorMessages shouldBe emptyList()
            converter.payloads shouldContainExactly listOf(message.payload)
            acknowledgement.acknowledged shouldBe true
        }

        "should publish conversion error when conversion fails" {
            val acknowledgement = Acknowledgement()
            val message = receivedMessage(
                acknowledge = acknowledgement::acknowledge
            )
            val receiver = FakeMessageReceiver(message)
            val publisher = FakeMessagePublisher()
            val converter = FakeOutgoingMessageConverter(
                Either.Left(AdditionalMessageInfoError("Could not resolve additional message info"))
            )

            val service = messageProcessingService(
                receiver = receiver,
                publisher = publisher,
                converter = converter,
                schemaValidator = FakeSchemaValidator()
            )

            service.processMessage(message)

            val errorMessage = publisher.errorMessages.single()
            errorMessage.sourceSystem shouldBe message.sourceSystem
            errorMessage.originalMessage.key shouldBe message.key
            errorMessage.originalMessage.payload shouldBe message.payload
            errorMessage.errors shouldContainExactly listOf(
                ProcessingError(
                    category = ErrorCategory.CONVERSION,
                    code = ErrorCode.CONVERSION_ERROR,
                    message = "Could not resolve additional message info"
                )
            )
            publisher.processedMessages shouldBe emptyList()
            converter.payloads shouldContainExactly listOf(message.payload)
            acknowledgement.acknowledged shouldBe true
        }

        "should not acknowledge message when publishing fails" {
            val acknowledgement = Acknowledgement()
            val message = receivedMessage(
                acknowledge = acknowledgement::acknowledge
            )
            val receiver = FakeMessageReceiver(message)
            val publishError = PublishError.Failure(
                referenceId = message.key.orEmpty(),
                topic = message.topic,
                cause = RuntimeException("Publish failed")
            )
            val publisher = FakeMessagePublisher(
                publishProcessedMessageResult = Either.Left(publishError)
            )
            val converter = FakeOutgoingMessageConverter(Either.Right("<xml />"))

            val service = messageProcessingService(
                receiver = receiver,
                publisher = publisher,
                converter = converter,
                schemaValidator = FakeSchemaValidator()
            )

            service.processMessage(message)

            publisher.processedMessages shouldContainExactly listOf(
                ProcessedMessage(
                    key = message.key.orEmpty(),
                    payload = "<xml />"
                )
            )
            publisher.errorMessages shouldBe emptyList()
            acknowledgement.acknowledged shouldBe false
        }
    }
)

private fun messageProcessingService(
    receiver: MessageReceiver,
    publisher: MessagePublisher,
    converter: OutgoingMessageConverter,
    schemaValidator: SchemaValidator = FakeSchemaValidator()
): MessageProcessingService =
    MessageProcessingService(
        messageReceiver = receiver,
        messagePublisher = publisher,
        messageValidator = MessageValidator(schemaValidator),
        outgoingMessageConverter = converter
    )

private fun receivedMessage(
    key: String? = Uuid.random().toString(),
    payload: String = """{"message":"valid"}""",
    sourceSystem: String? = "test-system",
    acknowledge: suspend () -> Unit = {}
): ReceivedMessage =
    ReceivedMessage(
        key = key,
        payload = payload,
        sourceSystem = sourceSystem,
        createdAt = Clock.System.now(),
        topic = "topic",
        partition = 0,
        offset = 0,
        acknowledge = acknowledge
    )

private class Acknowledgement {
    var acknowledged = false

    suspend fun acknowledge() {
        acknowledged = true
    }
}
