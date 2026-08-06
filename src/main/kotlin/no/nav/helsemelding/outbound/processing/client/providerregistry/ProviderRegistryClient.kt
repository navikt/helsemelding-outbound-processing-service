package no.nav.helsemelding.outbound.processing.client.providerregistry

import arrow.core.Either
import arrow.core.raise.either
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.request.get
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.client.statement.request
import io.ktor.http.ContentType.Application.Json
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import no.nav.helsemelding.messageconverter.msghead.model.provider.Provider
import no.nav.helsemelding.outbound.processing.client.providerregistry.model.ClientError
import no.nav.helsemelding.outbound.processing.client.providerregistry.model.HttpError
import no.nav.helsemelding.outbound.processing.client.providerregistry.model.UnexpectedClientError
import kotlin.uuid.Uuid

private val log = KotlinLogging.logger {}

private const val PROVIDER_PATH = "/api/v1/behandler"

fun interface ProviderRegistryClient {
    suspend fun getProvider(providerId: Uuid): Either<ClientError, Provider>
}

class HttpProviderRegistryClient(
    clientProvider: () -> HttpClient,
    private val providerRegistryBaseUrl: String
) : ProviderRegistryClient {
    private val httpClient = clientProvider()

    override suspend fun getProvider(providerId: Uuid): Either<ClientError, Provider> =
        either {
            val response = fetchProvider(providerId)
                .mapLeft { it.toUnexpectedError("Failed to request provider from ProviderRegistry") }
                .bind()

            log.debug { "Response from ${response.request.method} ${response.request.url} is ${response.status}" }

            if (response.status != HttpStatusCode.OK) {
                log.error {
                    "Request with url: $providerRegistryBaseUrl$PROVIDER_PATH/$providerId " +
                        "failed with response code: ${response.status.value}"
                }
                raise(response.toHttpError().bind())
            }

            response.toProvider()
                .mapLeft { it.toUnexpectedError("Failed to decode response from ProviderRegistry") }
                .bind()
        }

    private suspend fun fetchProvider(providerId: Uuid): Either<Throwable, HttpResponse> =
        Either.catch {
            httpClient.get("$providerRegistryBaseUrl$PROVIDER_PATH/$providerId") {
                contentType(Json)
            }
        }
}

private suspend fun HttpResponse.toHttpError(): Either<ClientError, HttpError> =
    either {
        HttpError(
            statusCode = status.value,
            message = Either.catch { bodyAsText() }
                .mapLeft { it.toUnexpectedError("Failed to read error response from ProviderRegistry") }
                .bind()
        )
    }

private suspend fun HttpResponse.toProvider(): Either<Throwable, Provider> =
    Either.catch { body<Provider>() }

private fun Throwable.toUnexpectedError(message: String): UnexpectedClientError =
    UnexpectedClientError(
        message = "$message: ${this.message ?: this::class.simpleName.orEmpty()}",
        cause = this
    )

class FakeProviderRegistryClient : ProviderRegistryClient {
    private val providerById = mutableMapOf<Uuid, Either<ClientError, Provider>>()

    fun givenProvider(uuid: Uuid, either: Either<ClientError, Provider>) {
        providerById[uuid] = either
    }

    override suspend fun getProvider(providerId: Uuid): Either<ClientError, Provider> {
        return providerById[providerId] ?: Either.Left(
            HttpError(
                statusCode = HttpStatusCode.Forbidden.value,
                message = "Error when fetching provider"
            )
        )
    }
}
