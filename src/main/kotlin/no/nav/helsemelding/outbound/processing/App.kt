package no.nav.helsemelding.outbound.processing

import arrow.continuations.SuspendApp
import arrow.continuations.ktor.server
import arrow.core.raise.result
import arrow.fx.coroutines.resourceScope
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.server.application.Application
import io.ktor.server.netty.Netty
import io.ktor.utils.io.CancellationException
import io.micrometer.prometheus.PrometheusMeterRegistry
import kotlinx.coroutines.awaitCancellation
import no.nav.helsemelding.outbound.processing.config.Config
import no.nav.helsemelding.outbound.processing.conversion.HttpAdditionalMessageInfoResolver
import no.nav.helsemelding.outbound.processing.conversion.MsgHeadOutgoingMessageConverter
import no.nav.helsemelding.outbound.processing.plugin.configureMetrics
import no.nav.helsemelding.outbound.processing.plugin.configureRoutes
import no.nav.helsemelding.outbound.processing.publisher.OutboundMessagePublisher
import no.nav.helsemelding.outbound.processing.receiver.OutboundMessageReceiver
import no.nav.helsemelding.outbound.processing.service.MessageProcessingService
import no.nav.helsemelding.outbound.processing.util.coroutineScope
import no.nav.helsemelding.outbound.processing.validation.MessageValidator

private val log = KotlinLogging.logger {}

fun main() = SuspendApp {
    result {
        resourceScope {
            val config = config()
            val deps = dependencies()

            val messageProcessingService = messageProcessingService(config, deps)

            server(
                Netty,
                port = config.server.port.value,
                preWait = config.server.preWait,
                module = processingServiceModule(deps.meterRegistry)
            )

            messageProcessingService.processMessages(coroutineScope())

            awaitCancellation()
        }
    }
        .onFailure { error -> if (error !is CancellationException) logError(error) }
}

private fun messageProcessingService(
    config: Config,
    deps: Dependencies
): MessageProcessingService {
    val additionalMessageInfoResolver = HttpAdditionalMessageInfoResolver(
        pdlClient = deps.pdlClient,
        providerRegistryClient = deps.providerRegistryClient
    )

    return MessageProcessingService(
        messageReceiver = OutboundMessageReceiver(
            dialogMessageOutJson = config.kafka.topics.dialogMessageOutJson,
            kafkaReceiver = deps.kafkaReceiver
        ),
        messagePublisher = OutboundMessagePublisher(
            topics = config.kafka.topics,
            kafkaPublisher = deps.kafkaPublisher
        ),
        messageValidator = MessageValidator(),
        outgoingMessageConverter = MsgHeadOutgoingMessageConverter(additionalMessageInfoResolver)
    )
}

internal fun processingServiceModule(
    meterRegistry: PrometheusMeterRegistry
): Application.() -> Unit {
    return {
        configureMetrics(meterRegistry)
        configureRoutes(meterRegistry)
    }
}

private fun logError(t: Throwable) = log.error(t) { "Shutdown outbound processing service" }
