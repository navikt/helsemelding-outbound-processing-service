package no.nav.helsemelding.outbound.processing.client.providerregistry

import arrow.core.Either
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.request.get
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.client.statement.request
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import no.nav.helsemelding.message.msghead.model.provider.Provider
import no.nav.helsemelding.outbound.processing.client.providerregistry.model.FetchingError
import no.nav.helsemelding.outbound.processing.client.providerregistry.model.ProviderError
import kotlin.uuid.Uuid

private val log = KotlinLogging.logger {}

class HttpProviderRegistryClient(
    clientProvider: () -> HttpClient,
    private val providerRegistryServiceUrl: String
) : ProviderRegistryClient {

    private val httpClient = clientProvider()

    override suspend fun getProvider(providerId: Uuid): Either<ProviderError, Provider> {
        val response = httpClient.get("$providerRegistryServiceUrl/api/v1/behandler/$providerId") {
            contentType(ContentType.Application.Json)
        }.withLogging()

        return when (response.status) {
            HttpStatusCode.OK -> Either.Right(response.body())
            else -> Either.Left(response.toFetchingError())
        }
    }
}

private suspend fun HttpResponse.toFetchingError() = FetchingError(
    code = status.value,
    message = bodyAsText()
)

private fun HttpResponse.withLogging(): HttpResponse {
    log.debug { "Response from ${request.method} ${request.url} is $status" }
    return this
}

interface ProviderRegistryClient {
    suspend fun getProvider(providerId: Uuid): Either<ProviderError, Provider>
}

class FakeProviderRegistryClient : ProviderRegistryClient {
    private val providerById = mutableMapOf<Uuid, Either<ProviderError, Provider>>()

    fun givenProvider(uuid: Uuid, either: Either<ProviderError, Provider>) {
        providerById[uuid] = either
    }

    override suspend fun getProvider(providerId: Uuid): Either<ProviderError, Provider> {
        return providerById[providerId] ?: Either.Left(
            FetchingError(
                code = HttpStatusCode.Forbidden.value,
                message = "Error when fetching provider"
            )
        )
    }
}
