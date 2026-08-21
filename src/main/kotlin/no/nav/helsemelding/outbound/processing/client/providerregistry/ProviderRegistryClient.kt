package no.nav.helsemelding.outbound.processing.client.providerregistry

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.raise.ensure
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
import no.nav.helsemelding.outbound.processing.client.providerregistry.model.ExternalProvider
import no.nav.helsemelding.outbound.processing.client.providerregistry.model.HttpError
import no.nav.helsemelding.outbound.processing.client.providerregistry.model.ProviderRegistryError
import no.nav.helsemelding.outbound.processing.client.providerregistry.model.UnexpectedError
import no.nav.helsemelding.outbound.processing.client.providerregistry.model.toProvider
import kotlin.uuid.Uuid

private val log = KotlinLogging.logger {}

private const val PROVIDER_PATH = "/api/v1/behandler"

fun interface ProviderRegistryClient {
    suspend fun getProvider(providerId: Uuid): Either<ProviderRegistryError, Provider>
}

class HttpProviderRegistryClient(
    clientProvider: () -> HttpClient,
    private val providerRegistryBaseUrl: String
) : ProviderRegistryClient {
    private val httpClient = clientProvider()

    override suspend fun getProvider(providerId: Uuid): Either<ProviderRegistryError, Provider> =
        either {
            val response = fetchProvider(providerId).bind()

            log.debug { "Response from ${response.request.method} ${response.request.url} is ${response.status}" }

            ensure(response.status == HttpStatusCode.OK) {
                log.error {
                    "Request with url: $providerRegistryBaseUrl$PROVIDER_PATH/$providerId " +
                        "failed with response code: ${response.status.value}"
                }
                response.toHttpError().bind()
            }

            response.body<ExternalProvider>().toProvider()
        }

    private suspend fun fetchProvider(providerId: Uuid): Either<ProviderRegistryError, HttpResponse> =
        Either.catch {
            httpClient.get("$providerRegistryBaseUrl$PROVIDER_PATH/$providerId") {
                contentType(Json)
            }
        }
            .mapLeft { it.toUnexpectedError("Failed to request provider from ProviderRegistry") }
}

private suspend fun HttpResponse.toHttpError(): Either<ProviderRegistryError, HttpError> =
    either {
        HttpError(
            statusCode = status.value,
            message = Either.catch { bodyAsText() }
                .mapLeft { it.toUnexpectedError("Failed to read error response from ProviderRegistry") }
                .bind()
        )
    }

private fun Throwable.toUnexpectedError(message: String): UnexpectedError =
    UnexpectedError(
        message = "$message: ${this.message ?: this::class.simpleName.orEmpty()}",
        cause = this
    )

class FakeProviderRegistryClient : ProviderRegistryClient {
    private val providerById = mutableMapOf<Uuid, Either<ProviderRegistryError, Provider>>()

    fun givenProvider(uuid: Uuid, either: Either<ProviderRegistryError, Provider>) {
        providerById[uuid] = either
    }

    override suspend fun getProvider(providerId: Uuid): Either<ProviderRegistryError, Provider> {
        return providerById[providerId] ?: Either.Left(
            HttpError(
                statusCode = HttpStatusCode.Forbidden.value,
                message = "Error when fetching provider"
            )
        )
    }
}
