package no.nav.helsemelding.outbound.processing.service

import arrow.core.Either
import arrow.core.getOrElse
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import no.nav.helsemelding.jsonschema.core.model.OutgoingDialogMessage
import no.nav.helsemelding.jsonschema.core.validation.SchemaValidator
import no.nav.helsemelding.messageconverter.json.OutgoingDialogMessageSerializer
import no.nav.helsemelding.outbound.processing.PublishError
import no.nav.helsemelding.outbound.processing.client.pdl.model.GraphQlError
import no.nav.helsemelding.outbound.processing.conversion.FakeOutgoingMessageConverter
import no.nav.helsemelding.outbound.processing.conversion.OutgoingMessageConverter
import no.nav.helsemelding.outbound.processing.conversion.OutgoingMessageError
import no.nav.helsemelding.outbound.processing.conversion.outgoingDialogMessage
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
import no.nav.helsemelding.payloadsigning.client.PayloadSigningClient
import no.nav.helsemelding.payloadsigning.model.Direction
import no.nav.helsemelding.payloadsigning.model.MessageSigningError
import no.nav.helsemelding.payloadsigning.model.PayloadRequest
import no.nav.helsemelding.payloadsigning.model.PayloadResponse
import kotlin.time.Clock

class MessageProcessingServiceSpec : StringSpec(
    {
        "should publish validation error when received message is invalid" {
            val acknowledgement = Acknowledgement()
            val message = receivedMessage(
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
            errorMessage.originalMessage.key shouldBe "null"
            errorMessage.originalMessage.payload shouldBe message.payload
            errorMessage.errors.map { it.code } shouldContainExactly listOf(
                ErrorCode.INVALID_KAFKA_VALUE,
                ErrorCode.MISSING_SOURCE_SYSTEM_HEADER
            )
            publisher.processedMessages shouldBe emptyList()
            converter.payloads shouldBe emptyList()
            acknowledgement.acknowledged shouldBe true
        }

        "should convert, sign and publish a message when it is valid" {
            val dialogMessage = outgoingDialogMessage()
            val acknowledgement = Acknowledgement()
            val message = receivedMessage(
                payload = dialogMessagePayload(dialogMessage),
                acknowledge = acknowledgement::acknowledge
            )
            val receiver = FakeMessageReceiver(message)
            val publisher = FakeMessagePublisher()
            val unsignedXml = "<xml>Unsigned message</xml>"
            val signedXml = "<xml>Signed message</xml>"
            val converter = FakeOutgoingMessageConverter(Either.Right(unsignedXml))
            val payloadSigningClient = FakePayloadSigningClient(
                result = { Either.Right(PayloadResponse(signedXml.encodeToByteArray())) }
            )

            val service = messageProcessingService(
                receiver = receiver,
                publisher = publisher,
                converter = converter,
                schemaValidator = FakeSchemaValidator(),
                payloadSigningClient = payloadSigningClient
            )

            service.processMessage(message)

            publisher.processedMessages.single() shouldBe ProcessedMessage(
                key = dialogMessage.id.toString(),
                payload = signedXml
            )
            payloadSigningClient.requests.single().direction shouldBe Direction.OUT
            payloadSigningClient.requests.single().bytes.decodeToString() shouldBe unsignedXml
            publisher.errorMessages shouldBe emptyList()
            converter.payloads shouldContainExactly listOf(message.payload)
            acknowledgement.acknowledged shouldBe true
        }

        "should publish signing error when payload signing fails" {
            val acknowledgement = Acknowledgement()
            val message = receivedMessage(
                acknowledge = acknowledgement::acknowledge
            )
            val receiver = FakeMessageReceiver(message)
            val publisher = FakeMessagePublisher()
            val converter = FakeOutgoingMessageConverter(Either.Right("<xml />"))
            val signingError = MessageSigningError(
                code = 500,
                message = "Payload signing failed"
            )
            val payloadSigningClient = FakePayloadSigningClient(
                result = { Either.Left(signingError) }
            )

            val service = messageProcessingService(
                receiver = receiver,
                publisher = publisher,
                converter = converter,
                schemaValidator = FakeSchemaValidator(),
                payloadSigningClient = payloadSigningClient
            )

            service.processMessage(message)

            publisher.processedMessages shouldBe emptyList()
            publisher.errorMessages.single().errors shouldContainExactly listOf(
                ProcessingError(
                    category = ErrorCategory.SIGNING,
                    code = ErrorCode.SIGNING_ERROR,
                    message = signingError.message
                )
            )
            acknowledgement.acknowledged shouldBe true
        }

        "should publish conversion error when conversion fails" {
            val dialogMessage = outgoingDialogMessage()
            val acknowledgement = Acknowledgement()
            val message = receivedMessage(
                payload = dialogMessagePayload(dialogMessage),
                acknowledge = acknowledgement::acknowledge
            )
            val receiver = FakeMessageReceiver(message)
            val publisher = FakeMessagePublisher()
            val processingError = ProcessingError(
                category = ErrorCategory.CONVERSION,
                code = ErrorCode.PDL_ERROR,
                message = "Could not resolve additional message info"
            )
            val converter = FakeOutgoingMessageConverter(
                Either.Left(
                    OutgoingMessageError.Client(
                        GraphQlError("Could not resolve additional message info")
                    )
                )
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
            errorMessage.originalMessage.key shouldBe dialogMessage.id.toString()
            errorMessage.originalMessage.payload shouldBe message.payload
            errorMessage.errors shouldContainExactly listOf(processingError)
            publisher.processedMessages shouldBe emptyList()
            converter.payloads shouldContainExactly listOf(message.payload)
            acknowledgement.acknowledged shouldBe true
        }

        "should publish conversion error when message id extraction fails" {
            val acknowledgement = Acknowledgement()
            val message = receivedMessage(
                payload = """{"not":"a valid outgoing dialog message"}""",
                acknowledge = acknowledgement::acknowledge
            )
            val receiver = FakeMessageReceiver(message)
            val publisher = FakeMessagePublisher()
            val converter = FakeOutgoingMessageConverter()

            val service = messageProcessingService(
                receiver = receiver,
                publisher = publisher,
                converter = converter,
                schemaValidator = FakeSchemaValidator()
            )

            service.processMessage(message)

            val errorMessage = publisher.errorMessages.single()
            errorMessage.sourceSystem shouldBe message.sourceSystem
            errorMessage.originalMessage.key shouldBe "null"
            errorMessage.originalMessage.payload shouldBe message.payload
            errorMessage.errors.single().category shouldBe ErrorCategory.CONVERSION
            errorMessage.errors.single().code shouldBe ErrorCode.MESSAGE_ID_EXTRACTION_ERROR
            publisher.processedMessages shouldBe emptyList()
            converter.payloads shouldBe emptyList()
            acknowledgement.acknowledged shouldBe true
        }

        "should not acknowledge message when publishing fails" {
            val dialogMessage = outgoingDialogMessage()
            val acknowledgement = Acknowledgement()
            val message = receivedMessage(
                payload = dialogMessagePayload(dialogMessage),
                acknowledge = acknowledgement::acknowledge
            )
            val receiver = FakeMessageReceiver(message)
            val publishError = PublishError.Failure(
                key = dialogMessage.id.toString(),
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
                    key = dialogMessage.id.toString(),
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
    schemaValidator: SchemaValidator = FakeSchemaValidator(),
    payloadSigningClient: PayloadSigningClient = FakePayloadSigningClient()
): MessageProcessingService =
    MessageProcessingService(
        messageReceiver = receiver,
        messagePublisher = publisher,
        messageValidator = MessageValidator(schemaValidator),
        outgoingMessageConverter = converter,
        payloadSigningClient = payloadSigningClient
    )

private fun receivedMessage(
    payload: String = dialogMessagePayload(),
    sourceSystem: String? = "test-system",
    acknowledge: suspend () -> Unit = {}
): ReceivedMessage =
    ReceivedMessage(
        payload = payload,
        sourceSystem = sourceSystem,
        createdAt = Clock.System.now(),
        topic = "topic",
        partition = 0,
        offset = 0,
        acknowledge = acknowledge
    )

private fun dialogMessagePayload(dialogMessage: OutgoingDialogMessage = outgoingDialogMessage()): String =
    OutgoingDialogMessageSerializer()
        .serialize(dialogMessage)
        .getOrElse { error("Could not serialize test OutgoingDialogMessage: ${it.message}") }

private class Acknowledgement {
    var acknowledged = false

    suspend fun acknowledge() {
        acknowledged = true
    }
}

private class FakePayloadSigningClient(
    private val result: (PayloadRequest) -> Either<MessageSigningError, PayloadResponse> = { request ->
        Either.Right(PayloadResponse(request.bytes))
    }
) : PayloadSigningClient {
    val requests = mutableListOf<PayloadRequest>()

    override suspend fun signPayload(payloadRequest: PayloadRequest): Either<MessageSigningError, PayloadResponse> {
        requests.add(payloadRequest)
        return result(payloadRequest)
    }

    override fun close() = Unit
}
