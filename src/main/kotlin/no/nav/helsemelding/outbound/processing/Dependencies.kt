package no.nav.helsemelding.outbound.processing

import arrow.fx.coroutines.ExitCase
import arrow.fx.coroutines.ResourceScope
import arrow.fx.coroutines.await.awaitAll
import io.github.nomisRev.kafka.publisher.KafkaPublisher
import io.github.nomisRev.kafka.receiver.AutoOffsetReset
import io.github.nomisRev.kafka.receiver.KafkaReceiver
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.client.HttpClient
import io.micrometer.prometheus.PrometheusConfig.DEFAULT
import io.micrometer.prometheus.PrometheusMeterRegistry
import no.nav.helsemelding.outbound.processing.client.auth.httpTokenClient
import no.nav.helsemelding.outbound.processing.client.auth.scopedAuthHttpClient
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

internal suspend fun ResourceScope.tokenHttpClient(): HttpClient =
    install({ httpTokenClient(config().httpTokenClient) }) { c, _: ExitCase ->
        c.close().also { log.info { "Closed http token client" } }
    }

internal suspend fun ResourceScope.scopedApiHttpClient(tokenClient: HttpClient, scope: String): HttpClient =
    install(
        {
            scopedAuthHttpClient(
                tokenClient = tokenClient,
                azureAuth = config().azureAuth,
                httpClientConfig = config().httpClient,
                scope = scope
            )
        }
    ) { c, _: ExitCase -> c.close().also { log.info { "Closed scoped auth http client" } } }

internal fun kafkaReceiver(kafka: Kafka, autoOffsetReset: AutoOffsetReset): KafkaReceiver<String, ByteArray> =
    KafkaReceiver(kafka.toReceiverSettings(autoOffsetReset))

internal fun pdlClient(pdl: Pdl, httpClient: HttpClient): PdlClient =
    HttpPdlClient(
        clientProvider = { httpClient },
        pdlGraphqlUrl = pdl.graphqlUrl,
        processingNumber = pdl.processingNumber
    )

internal fun providerRegistryClient(
    providerRegistry: ProviderRegistry,
    httpClient: HttpClient
): ProviderRegistryClient =
    HttpProviderRegistryClient(
        clientProvider = { httpClient },
        providerRegistryBaseUrl = providerRegistry.baseUrl
    )

suspend fun ResourceScope.dependencies(): Dependencies = awaitAll {
    val config = config()

    val metricsRegistry = async { metricsRegistry() }
    val kafkaReceiver = kafkaReceiver(config.kafka, AutoOffsetReset.Latest)
    val kafkaPublisher = async { kafkaPublisher(config.kafka) }
    val tokenClient = async { tokenHttpClient() }
    val pdlHttpClient = async { scopedApiHttpClient(tokenClient.await(), config.pdl.scope) }
    val providerRegistryHttpClient = async { scopedApiHttpClient(tokenClient.await(), config.providerRegistry.scope) }
    val pdlClient = async { pdlClient(config.pdl, pdlHttpClient.await()) }
    val providerRegistryClient = async {
        providerRegistryClient(config.providerRegistry, providerRegistryHttpClient.await())
    }

    Dependencies(
        meterRegistry = metricsRegistry.await(),
        kafkaReceiver = kafkaReceiver,
        kafkaPublisher = kafkaPublisher.await(),
        pdlClient = pdlClient.await(),
        providerRegistryClient = providerRegistryClient.await()
    )
}
