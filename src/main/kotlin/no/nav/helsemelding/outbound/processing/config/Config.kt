package no.nav.helsemelding.outbound.processing.config

import com.sksamuel.hoplite.Masked
import io.github.nomisRev.kafka.publisher.PublisherSettings
import io.github.nomisRev.kafka.receiver.AutoOffsetReset
import io.github.nomisRev.kafka.receiver.ReceiverSettings
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties
import kotlin.time.Duration

data class Config(
    val kafka: Kafka,
    val pdl: Pdl,
    val providerRegistry: ProviderRegistry,
    val providerHerIdOverride: ProviderHerIdOverride,
    val server: Server,
    val azureAuth: AzureAuth,
    val httpClient: HttpClientConfig
)

data class Kafka(
    val groupId: String,
    val bootstrapServers: String,
    val securityProtocol: SecurityProtocol,
    val keystoreType: KeystoreType,
    val keystoreLocation: KeystoreLocation,
    val keystorePassword: Masked,
    val truststoreType: TruststoreType,
    val truststoreLocation: TruststoreLocation,
    val truststorePassword: Masked,
    val topics: Topics
) {
    private val securityProtocolConfig = "security.protocol"
    private val sslKeystoreTypeConfig = "ssl.keystore.type"
    private val sslKeystoreLocationConfig = "ssl.keystore.location"
    private val sslKeystorePasswordConfig = "ssl.keystore.password"
    private val sslTruststoreTypeConfig = "ssl.truststore.type"
    private val sslTruststoreLocationConfig = "ssl.truststore.location"
    private val sslTruststorePasswordConfig = "ssl.truststore.password"

    @JvmInline
    value class SecurityProtocol(val value: String)

    @JvmInline
    value class KeystoreType(val value: String)

    @JvmInline
    value class KeystoreLocation(val value: String)

    @JvmInline
    value class TruststoreType(val value: String)

    @JvmInline
    value class TruststoreLocation(val value: String)

    data class Topics(
        val dialogMessageOutJson: String,
        val dialogMessageOutXml: String,
        val errorMessage: String
    )

    fun toPublisherSettings(): PublisherSettings<String, ByteArray> =
        PublisherSettings(
            bootstrapServers = bootstrapServers,
            keySerializer = StringSerializer(),
            valueSerializer = ByteArraySerializer(),
            properties = toProperties()
        )

    fun toReceiverSettings(autoOffsetReset: AutoOffsetReset): ReceiverSettings<String, ByteArray> =
        ReceiverSettings(
            bootstrapServers = bootstrapServers,
            keyDeserializer = StringDeserializer(),
            valueDeserializer = ByteArrayDeserializer(),
            groupId = groupId,
            properties = toProperties(),
            autoOffsetReset = autoOffsetReset
        )

    private fun toProperties() = Properties()
        .apply {
            put(securityProtocolConfig, securityProtocol.value)
            put(sslKeystoreTypeConfig, keystoreType.value)
            put(sslKeystoreLocationConfig, keystoreLocation.value)
            put(sslKeystorePasswordConfig, keystorePassword.value)
            put(sslTruststoreTypeConfig, truststoreType.value)
            put(sslTruststoreLocationConfig, truststoreLocation.value)
            put(sslTruststorePasswordConfig, truststorePassword.value)
        }
}

fun Config.withKafka(update: Kafka.() -> Kafka) = copy(kafka = kafka.update())

data class Pdl(
    val graphqlUrl: String,
    val processingNumber: String,
    val scope: String
)

data class ProviderRegistry(
    val baseUrl: String,
    val scope: String
)

data class ProviderHerIdOverride(
    val herId: Int? = null,
    val officeHerId: Int? = null
)

data class Server(
    val port: Port,
    val preWait: Duration
) {
    @JvmInline
    value class Port(val value: Int)
}

data class AzureAuth(
    val azureTokenEndpoint: AzureTokenEndpoint,
    val azureAppClientId: AzureAppClientId,
    val azureAppClientSecret: Masked,
    val azureGrantType: AzureGrantType
) {
    @JvmInline
    value class AzureTokenEndpoint(val value: String)

    @JvmInline
    value class AzureAppClientId(val value: String)

    @JvmInline
    value class AzureGrantType(val value: String)
}

data class HttpClientConfig(
    val maxConnectionsCount: Int,
    val connectionTimeout: Duration,
    val requestTimeout: Duration,
    val socketTimeout: Duration,
    val keepAliveTime: Duration
)
