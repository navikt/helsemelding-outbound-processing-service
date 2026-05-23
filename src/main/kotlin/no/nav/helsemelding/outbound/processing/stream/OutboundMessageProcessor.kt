package no.nav.helsemelding.outbound.processing.stream

import no.nav.helsemelding.outbound.processing.model.ErrorMessage
import no.nav.helsemelding.outbound.processing.model.OriginalMessage
import org.apache.kafka.streams.processor.api.FixedKeyProcessor
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext
import org.apache.kafka.streams.processor.api.FixedKeyRecord
import kotlin.time.Clock
import kotlin.time.Instant

data class ProcessedMessage(
    val key: String?,
    val payload: ByteArray,
    val createdAt: Instant,
    val processedAt: Instant,
    val validation: OutboundMessageValidation
) {
    fun isValid(): Boolean = validation.isValid()

    fun errors(): List<ErrorMessage> =
        validation.errors().map { error ->
            ErrorMessage(
                processedAt = processedAt,
                error = error,
                originalMessage = OriginalMessage(
                    createdAt = createdAt,
                    key = key.orEmpty(),
                    payload = payload.decodeToString()
                )
            )
        }
}

class OutboundMessageProcessor(
    private val validator: OutboundMessageValidator
) : FixedKeyProcessor<String, ByteArray, ProcessedMessage> {
    private lateinit var context: FixedKeyProcessorContext<String, ProcessedMessage>

    override fun init(context: FixedKeyProcessorContext<String, ProcessedMessage>) {
        this.context = context
    }

    override fun process(record: FixedKeyRecord<String, ByteArray>) {
        val validation = validator.validate(
            key = record.key(),
            value = record.value(),
            headers = record.headers()
        )

        context.forward(
            record.withValue(
                ProcessedMessage(
                    key = record.key(),
                    payload = record.value(),
                    validation = validation,
                    createdAt = Instant.fromEpochMilliseconds(record.timestamp()),
                    processedAt = Clock.System.now()
                )
            )
        )
    }

    override fun close() {}
}
