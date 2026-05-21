package no.nav.helsemelding.outbound.processing

import arrow.fx.coroutines.ExitCase
import arrow.fx.coroutines.ResourceScope
import arrow.fx.coroutines.await.awaitAll
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.prometheus.PrometheusConfig.DEFAULT
import io.micrometer.prometheus.PrometheusMeterRegistry
import no.nav.helsemelding.outbound.processing.config.KafkaStreamsSettings
import no.nav.helsemelding.outbound.processing.stream.OutboundMessageTopology
import no.nav.helsemelding.outbound.processing.stream.OutboundMessageValidator
import org.apache.kafka.streams.KafkaStreams

private val log = KotlinLogging.logger {}

data class Dependencies(
    val meterRegistry: PrometheusMeterRegistry,
    val kafkaStreams: KafkaStreams
)

internal suspend fun ResourceScope.metricsRegistry(): PrometheusMeterRegistry =
    install({ PrometheusMeterRegistry(DEFAULT) }) { p, _: ExitCase ->
        p.close().also { log.info { "Closed prometheus registry" } }
    }

internal suspend fun ResourceScope.kafkaStreams(kafkaStreamsSettings: KafkaStreamsSettings): KafkaStreams =
    install({ kafkaStreamsSettings.toKafkaStreams(outboundMessageTopology()).apply { start() } }) { ks, _: ExitCase ->
        ks.close().also { log.info { "Closed kafka streams" } }
    }

suspend fun ResourceScope.dependencies(): Dependencies = awaitAll {
    val config = config()

    val metricsRegistry = async { metricsRegistry() }
    val kafkaStreams = async { kafkaStreams(config.kafkaStreamsSettings) }

    Dependencies(
        metricsRegistry.await(),
        kafkaStreams.await()
    )
}

private fun outboundMessageTopology(): OutboundMessageTopology =
    OutboundMessageTopology(
        OutboundMessageValidator()
    )
