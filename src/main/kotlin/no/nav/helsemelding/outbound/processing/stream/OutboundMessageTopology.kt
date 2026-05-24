package no.nav.helsemelding.outbound.processing.stream

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.serialization.json.Json
import no.nav.helsemelding.outbound.processing.config
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier

private val log = KotlinLogging.logger {}

class OutboundMessageTopology(
    private val validator: OutboundMessageValidator
) {
    fun build(): Topology {
        val builder = StreamsBuilder()

        val processed = builder.processOutboundMessages()

        processed.routeValidMessages()
        processed.routeInvalidMessages()

        return builder.build()
    }

    private fun StreamsBuilder.processOutboundMessages(): KStream<String, ProcessedMessage> =
        stream<String, String>(config().kafkaStreamsSettings.topics.dialogMessageIn)
            .peek { key, value ->
                log.info {
                    "Received message: key=$key payload=$value"
                }
            }
            .processValues(
                FixedKeyProcessorSupplier {
                    OutboundMessageProcessor(validator)
                }
            )

    private fun KStream<String, ProcessedMessage>.routeValidMessages() {
        filter { _, value -> value.isValid() }
            .peek { key, value ->
                log.info {
                    "Message passed outbound validation: key=$key payload${value.payload}"
                }
            }
            .toXmlPayload()
            .to(config().kafkaStreamsSettings.topics.dialogMessageOut)
    }

    private fun KStream<String, ProcessedMessage>.routeInvalidMessages() {
        filterNot { _, value -> value.isValid() }
            .peek { key, value ->
                log.warn {
                    val errors = value.errors()
                        .joinToString { errorMessage ->
                            "${errorMessage.error.code}: ${errorMessage.error.message}"
                        }
                    "Message rejected by outbound validation: key=$key errors=[$errors]"
                }
            }
            .flatMapValues(ProcessedMessage::errors)
            .mapValues { errorMessage ->
                Json.encodeToString(errorMessage)
            }
            .to(config().kafkaStreamsSettings.topics.dialogMessageError)
    }

    private fun KStream<String, ProcessedMessage>.toXmlPayload(): KStream<String, String> =
        mapValues { message ->
            // jsonToXmlMapper.toXml(message.payload)
            message.payload
        }
}
