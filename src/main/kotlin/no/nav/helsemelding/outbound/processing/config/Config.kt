package no.nav.helsemelding.outbound.processing.config

import com.sksamuel.hoplite.Masked
import no.nav.helsemelding.outbound.processing.stream.OutboundMessageTopology
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.serialization.Serdes.ByteArraySerde
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import java.util.Properties
import kotlin.time.Duration

data class Config(
    val kafkaStreamsSettings: KafkaStreamsSettings,
    val server: Server
)

data class KafkaStreamsSettings(
    val applicationId: String,
    val bootstrapServers: String,
    val defaultKeySerde: KeySerde = KeySerde(),
    val defaultValueSerde: ValueSerde = ValueSerde(),
    val securityProtocol: SecurityProtocol,
    val keystoreType: KeystoreType,
    val keystoreLocation: KeystoreLocation,
    val keystorePassword: Masked,
    val truststoreType: TruststoreType,
    val truststoreLocation: TruststoreLocation,
    val truststorePassword: Masked,
    val topics: Topics
) {
    private val sslKeystoreTypeConfig = "ssl.keystore.type"
    private val sslKeystoreLocationConfig = "ssl.keystore.location"
    private val sslKeystorePasswordConfig = "ssl.keystore.password"
    private val sslTruststoreTypeConfig = "ssl.truststore.type"
    private val sslTruststoreLocationConfig = "ssl.truststore.location"
    private val sslTruststorePasswordConfig = "ssl.truststore.password"

    @JvmInline
    value class KeySerde(val value: String = StringSerde().javaClass.name)

    @JvmInline
    value class ValueSerde(val value: String = ByteArraySerde().javaClass.name)

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

    fun toKafkaStreams(topology: OutboundMessageTopology): KafkaStreams =
        KafkaStreams(
            topology.build(),
            toProperties()
        )

    fun toProperties() = Properties()
        .apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, defaultKeySerde.value)
            put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, defaultValueSerde.value)
            put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.value)
            put(sslKeystoreTypeConfig, keystoreType.value)
            put(sslKeystoreLocationConfig, keystoreLocation.value)
            put(sslKeystorePasswordConfig, keystorePassword.value)
            put(sslTruststoreTypeConfig, truststoreType.value)
            put(sslTruststoreLocationConfig, truststoreLocation.value)
            put(sslTruststorePasswordConfig, truststorePassword.value)
        }
}

data class Topics(
    val dialogMessageIn: String,
    val dialogMessageOut: String,
    val dialogMessageError: String
)

data class Server(
    val port: Port,
    val preWait: Duration
) {
    @JvmInline
    value class Port(val value: Int)
}
