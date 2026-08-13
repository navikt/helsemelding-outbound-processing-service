package no.nav.helsemelding.outbound.processing.receiver

import io.github.nomisRev.kafka.receiver.KafkaReceiver
import io.github.nomisRev.kafka.receiver.ReceiverRecord
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import no.nav.helsemelding.outbound.processing.model.ReceivedMessage
import kotlin.time.Instant

private const val SOURCE_SYSTEM_HEADER = "sourceSystem"

interface MessageReceiver {
    fun receiveMessages(): Flow<ReceivedMessage>
}

class OutboundMessageReceiver(
    private val dialogMessageOutJson: String,
    private val kafkaReceiver: KafkaReceiver<String, ByteArray>
) : MessageReceiver {
    override fun receiveMessages(): Flow<ReceivedMessage> =
        kafkaReceiver
            .receive(dialogMessageOutJson)
            .map(::toMessage)

    private fun toMessage(record: ReceiverRecord<String, ByteArray>): ReceivedMessage =
        ReceivedMessage(
            key = record.key(),
            payload = record.value().decodeToString(),
            sourceSystem = record.headers()
                .lastHeader(SOURCE_SYSTEM_HEADER)
                ?.value()
                ?.decodeToString(),
            createdAt = Instant.fromEpochMilliseconds(record.timestamp()),
            topic = record.topic(),
            partition = record.partition(),
            offset = record.offset(),
            acknowledge = { record.offset.acknowledge() }
        )
}

class FakeMessageReceiver(
    private vararg val messages: ReceivedMessage
) : MessageReceiver {
    override fun receiveMessages(): Flow<ReceivedMessage> = flowOf(*messages)
}
