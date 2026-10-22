package no.nav.helsemelding.outbound.processing.client.providerregistry.model

import no.nav.helsemelding.jsonschema.core.model.ErrorCode.PROVIDER_REGISTRY_ERROR
import no.nav.helsemelding.outbound.processing.client.ClientError

sealed interface ProviderRegistryError : ClientError {
    override val code get() = PROVIDER_REGISTRY_ERROR
}

data class HttpError(
    val statusCode: Int,
    override val message: String
) : ProviderRegistryError

data class UnexpectedError(
    override val message: String,
    val cause: Throwable
) : ProviderRegistryError
