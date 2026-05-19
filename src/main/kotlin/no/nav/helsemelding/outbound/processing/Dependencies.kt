package no.nav.helsemelding.outbound.processing

import arrow.fx.coroutines.ExitCase
import arrow.fx.coroutines.ResourceScope
import arrow.fx.coroutines.await.awaitAll
import io.github.nomisRev.kafka.publisher.KafkaPublisher
import io.github.nomisRev.kafka.receiver.AutoOffsetReset
import io.github.nomisRev.kafka.receiver.KafkaReceiver
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.prometheus.PrometheusConfig.DEFAULT
import io.micrometer.prometheus.PrometheusMeterRegistry
import no.nav.helsemelding.outbound.processing.config.Kafka

private val log = KotlinLogging.logger {}

data class Dependencies(
    val meterRegistry: PrometheusMeterRegistry,
    val kafkaReceiver: KafkaReceiver<String, ByteArray>,
    val kafkaPublisher: KafkaPublisher<String, ByteArray>
)

internal suspend fun ResourceScope.metricsRegistry(): PrometheusMeterRegistry =
    install({ PrometheusMeterRegistry(DEFAULT) }) { p, _: ExitCase ->
        p.close().also { log.info { "Closed prometheus registry" } }
    }

internal suspend fun ResourceScope.kafkaPublisher(kafka: Kafka): KafkaPublisher<String, ByteArray> =
    install({ KafkaPublisher(kafka.toPublisherSettings()) }) { p, _: ExitCase ->
        p.close().also { log.info { "Closed kafka publisher" } }
    }

internal fun kafkaReceiver(kafka: Kafka, autoOffsetReset: AutoOffsetReset): KafkaReceiver<String, ByteArray> =
    KafkaReceiver(kafka.toReceiverSettings(kafka, autoOffsetReset))

suspend fun ResourceScope.dependencies(): Dependencies = awaitAll {
    val config = config()

    val metricsRegistry = async { metricsRegistry() }
    val kafkaPublisher = async { kafkaPublisher(config.kafka) }

    val kafkaReceiver = kafkaReceiver(config.kafka, AutoOffsetReset.Latest)

    Dependencies(
        metricsRegistry.await(),
        kafkaReceiver,
        kafkaPublisher.await()
    )
}
