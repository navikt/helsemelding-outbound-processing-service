package no.nav.helsemelding.outbound.processing

import arrow.fx.coroutines.ExitCase
import arrow.fx.coroutines.ResourceScope
import arrow.fx.coroutines.await.awaitAll
import io.github.nomisRev.kafka.publisher.KafkaPublisher
import io.github.nomisRev.kafka.receiver.AutoOffsetReset
import io.github.nomisRev.kafka.receiver.KafkaReceiver
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.serialization.kotlinx.json.json
import io.micrometer.prometheus.PrometheusConfig.DEFAULT
import io.micrometer.prometheus.PrometheusMeterRegistry
import kotlinx.serialization.json.Json
import no.nav.helsemelding.outbound.processing.client.pdl.HttpPdlClient
import no.nav.helsemelding.outbound.processing.client.pdl.PdlClient
import no.nav.helsemelding.outbound.processing.client.providerregistry.HttpProviderRegistryClient
import no.nav.helsemelding.outbound.processing.client.providerregistry.ProviderRegistryClient
import no.nav.helsemelding.outbound.processing.config.Kafka
import no.nav.helsemelding.outbound.processing.config.Pdl
import no.nav.helsemelding.outbound.processing.config.ProviderRegistry

private val log = KotlinLogging.logger {}

data class Dependencies(
    val meterRegistry: PrometheusMeterRegistry,
    val kafkaReceiver: KafkaReceiver<String, ByteArray>,
    val kafkaPublisher: KafkaPublisher<String, ByteArray>,
    val pdlClient: PdlClient,
    val providerRegistryClient: ProviderRegistryClient
)

internal suspend fun ResourceScope.metricsRegistry(): PrometheusMeterRegistry =
    install({ PrometheusMeterRegistry(DEFAULT) }) { p, _: ExitCase ->
        p.close().also { log.info { "Closed prometheus registry" } }
    }

internal suspend fun ResourceScope.kafkaPublisher(kafka: Kafka): KafkaPublisher<String, ByteArray> =
    install({ KafkaPublisher(kafka.toPublisherSettings()) }) { p, _: ExitCase ->
        p.close().also { log.info { "Closed kafka publisher" } }
    }

internal suspend fun ResourceScope.httpClient(): HttpClient =
    install(
        {
            HttpClient(CIO) {
                install(ContentNegotiation) {
                    json(
                        Json {
                            ignoreUnknownKeys = true
                        }
                    )
                }
            }
        }
    ) { c, _: ExitCase -> c.close().also { log.info { "Closed http client" } } }

internal fun kafkaReceiver(kafka: Kafka, autoOffsetReset: AutoOffsetReset): KafkaReceiver<String, ByteArray> =
    KafkaReceiver(kafka.toReceiverSettings(autoOffsetReset))

internal fun pdlClient(pdl: Pdl, httpClient: HttpClient): PdlClient =
    HttpPdlClient(
        clientProvider = { httpClient },
        pdlGraphqlUrl = pdl.graphqlUrl
    )

internal fun providerRegistryClient(providerRegistry: ProviderRegistry, httpClient: HttpClient): ProviderRegistryClient =
    HttpProviderRegistryClient(
        clientProvider = { httpClient },
        providerRegistryBaseUrl = providerRegistry.baseUrl
    )

suspend fun ResourceScope.dependencies(): Dependencies = awaitAll {
    val config = config()

    val metricsRegistry = async { metricsRegistry() }
    val kafkaReceiver = kafkaReceiver(config.kafka, AutoOffsetReset.Latest)
    val kafkaPublisher = async { kafkaPublisher(config.kafka) }
    val httpClient = async { httpClient() }
    val client = httpClient.await()
    val pdlClient = async { pdlClient(config.pdl, client) }
    val providerRegistryClient = async { providerRegistryClient(config.providerRegistry, client) }

    Dependencies(
        meterRegistry = metricsRegistry.await(),
        kafkaReceiver = kafkaReceiver,
        kafkaPublisher = kafkaPublisher.await(),
        pdlClient = pdlClient.await(),
        providerRegistryClient = providerRegistryClient.await()
    )
}
