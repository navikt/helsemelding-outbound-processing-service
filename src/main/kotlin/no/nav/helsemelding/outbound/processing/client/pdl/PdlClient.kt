package no.nav.helsemelding.outbound.processing.client.pdl

import arrow.core.Either
import arrow.core.raise.either
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.client.statement.request
import io.ktor.http.ContentType.Application.Json
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import no.nav.helsemelding.messageconverter.msghead.model.Personident
import no.nav.helsemelding.outbound.processing.client.pdl.model.ClientError
import no.nav.helsemelding.outbound.processing.client.pdl.model.GraphQlError
import no.nav.helsemelding.outbound.processing.client.pdl.model.HttpError
import no.nav.helsemelding.outbound.processing.client.pdl.model.PersonName
import no.nav.helsemelding.outbound.processing.client.pdl.model.PersonNotFound
import no.nav.helsemelding.outbound.processing.client.pdl.model.PersonRequest
import no.nav.helsemelding.outbound.processing.client.pdl.model.PersonResponse
import no.nav.helsemelding.outbound.processing.client.pdl.model.PersonVariables
import no.nav.helsemelding.outbound.processing.client.pdl.model.UnexpectedClientError
import no.nav.helsemelding.outbound.processing.client.pdl.model.errorMessage

private val log = KotlinLogging.logger {}

private const val PROCESSING_NUMBER_HEADER_KEY = "Behandlingsnummer"

private val PDL_PERSON_QUERY = """
    query(${'$'}ident: ID!, ${'$'}navnHistorikk: Boolean!){
      hentPerson(ident: ${'$'}ident) {
        navn(historikk: ${'$'}navnHistorikk) {
          fornavn
          mellomnavn
          etternavn
          forkortetNavn
          originaltNavn {
            fornavn
            mellomnavn
            etternavn
          }
        }
      }
    }
""".trimIndent()

fun interface PdlClient {
    suspend fun getPersonName(personident: Personident): Either<ClientError, PersonName>
}

class HttpPdlClient(
    clientProvider: () -> HttpClient,
    private val pdlGraphqlUrl: String,
    private val processingNumber: String
) : PdlClient {
    private val httpClient = clientProvider()

    override suspend fun getPersonName(personident: Personident): Either<ClientError, PersonName> =
        either {
            val response = fetchPerson(personident)
                .mapLeft { it.toUnexpectedError("Failed to request person from PersonDataLosningen") }
                .bind()

            log.debug { "Response from ${response.request.method} ${response.request.url} is ${response.status}" }

            if (response.status != HttpStatusCode.OK) {
                log.error { "Request with url: $pdlGraphqlUrl failed with response code: ${response.status.value}" }
                raise(response.toFetchingError().bind())
            }

            response.toPersonResponse()
                .mapLeft { it.toUnexpectedError("Failed to decode response from PersonDataLosningen") }
                .bind()
                .toPersonName()
                .bind()
        }

    private suspend fun fetchPerson(personident: Personident): Either<Throwable, HttpResponse> =
        Either.catch {
            httpClient.post(pdlGraphqlUrl) {
                contentType(Json)
                header(PROCESSING_NUMBER_HEADER_KEY, processingNumber)
                setBody(
                    PersonRequest(
                        query = PDL_PERSON_QUERY,
                        variables = PersonVariables(
                            nationalIdentityNumber = personident.value
                        )
                    )
                )
            }
        }
}

private fun PersonResponse.toPersonName(): Either<ClientError, PersonName> =
    either {
        errors
            .takeIf { it.isNotEmpty() }
            ?.let { pdlErrors ->
                val errorMessage = "Error while requesting person from PersonDataLosningen"
                pdlErrors.forEach { log.error { "$errorMessage: ${it.errorMessage()}" } }
                raise(GraphQlError(errorMessage, pdlErrors))
            }

        val person = data?.person ?: raise(PersonNotFound("Person not found: data or hentPerson is null"))

        person.names.firstOrNull() ?: raise(PersonNotFound("Person not found: navn empty"))
    }

private suspend fun HttpResponse.toFetchingError(): Either<ClientError, HttpError> =
    either {
        HttpError(
            statusCode = status.value,
            message = Either.catch { bodyAsText() }
                .mapLeft { it.toUnexpectedError("Failed to read error response from PersonDataLosningen") }
                .bind()
        )
    }

private suspend fun HttpResponse.toPersonResponse(): Either<Throwable, PersonResponse> =
    Either.catch { body<PersonResponse>() }

private fun Throwable.toUnexpectedError(message: String): UnexpectedClientError =
    UnexpectedClientError(
        message = "$message: ${this.message ?: this::class.simpleName.orEmpty()}",
        cause = this
    )

class FakePdlClient : PdlClient {
    private val personNameByIdent = mutableMapOf<Personident, Either<ClientError, PersonName>>()

    fun givenPersonName(personident: Personident, either: Either<ClientError, PersonName>) {
        personNameByIdent[personident] = either
    }

    override suspend fun getPersonName(personident: Personident): Either<ClientError, PersonName> {
        return personNameByIdent[personident] ?: Either.Left(GraphQlError("Error when fetching person name"))
    }
}
