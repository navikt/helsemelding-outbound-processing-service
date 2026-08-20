package no.nav.helsemelding.outbound.processing.client.pdl

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.raise.ensure
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
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import no.nav.helsemelding.messageconverter.msghead.model.Personident
import no.nav.helsemelding.outbound.processing.client.auth.AccessTokenProvider
import no.nav.helsemelding.outbound.processing.client.pdl.model.GraphQlError
import no.nav.helsemelding.outbound.processing.client.pdl.model.HttpError
import no.nav.helsemelding.outbound.processing.client.pdl.model.PdlError
import no.nav.helsemelding.outbound.processing.client.pdl.model.PersonName
import no.nav.helsemelding.outbound.processing.client.pdl.model.PersonNotFound
import no.nav.helsemelding.outbound.processing.client.pdl.model.PersonRequest
import no.nav.helsemelding.outbound.processing.client.pdl.model.PersonResponse
import no.nav.helsemelding.outbound.processing.client.pdl.model.PersonVariables
import no.nav.helsemelding.outbound.processing.client.pdl.model.UnexpectedError
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
        }
      }
    }
""".trimIndent()

fun interface PdlClient {
    suspend fun getPersonName(personident: Personident): Either<PdlError, PersonName>
}

class HttpPdlClient(
    clientProvider: () -> HttpClient,
    private val scope: String,
    private val pdlGraphqlUrl: String,
    private val processingNumber: String,
    private val tokenProvider: AccessTokenProvider
) : PdlClient {
    private val httpClient = clientProvider()

    override suspend fun getPersonName(personident: Personident): Either<PdlError, PersonName> =
        either {
            val personResponse = fetchPersonResponse(
                personident = personident,
                accessToken = tokenProvider.token(scope)
            )
                .bind()

            personResponse.toPersonName().bind()
        }

    private suspend fun fetchPersonResponse(
        personident: Personident,
        accessToken: String
    ): Either<PdlError, PersonResponse> =
        either {
            val response = fetchPerson(personident, accessToken).bind()

            log.debug { "Response from ${response.request.method} ${response.request.url} is ${response.status}" }

            ensure(response.status == HttpStatusCode.OK) {
                log.error { "Request with url: $pdlGraphqlUrl failed with response code: ${response.status.value}" }
                response.toFetchingError().bind()
            }

            response.body<PersonResponse>()
        }

    private suspend fun fetchPerson(
        personident: Personident,
        accessToken: String
    ): Either<PdlError, HttpResponse> =
        Either.catch {
            httpClient.post(pdlGraphqlUrl) {
                contentType(Json)
                header(HttpHeaders.Authorization, "Bearer $accessToken")
                header(PROCESSING_NUMBER_HEADER_KEY, processingNumber)
                setBody(
                    PersonRequest(
                        query = PDL_PERSON_QUERY,
                        variables = PersonVariables(
                            nationalIdentityNumber = personident.value,
                            includeNameHistory = false
                        )
                    )
                )
            }
        }
            .mapLeft { it.toUnexpectedError("Failed to request person from PDL") }
}

private fun PersonResponse.toPersonName(): Either<PdlError, PersonName> =
    either {
        ensure(errors.isEmpty()) {
            val errorMessage = "Error while requesting person from PDL"
            errors.forEach { log.error { "$errorMessage: ${it.errorMessage()}" } }
            GraphQlError(errorMessage, errors)
        }

        val name = data?.person?.names?.firstOrNull()
        ensure(name != null) { PersonNotFound("Person not found") }
        name
    }

private suspend fun HttpResponse.toFetchingError(): Either<PdlError, HttpError> =
    either {
        HttpError(
            statusCode = status.value,
            message = Either.catch { bodyAsText() }
                .mapLeft { it.toUnexpectedError("Failed to read error response from PDL") }
                .bind()
        )
    }

private fun Throwable.toUnexpectedError(message: String): UnexpectedError =
    UnexpectedError(
        message = "$message: ${this.message ?: this::class.simpleName.orEmpty()}",
        cause = this
    )

class FakePdlClient : PdlClient {
    private val personNameByIdent = mutableMapOf<Personident, Either<PdlError, PersonName>>()

    fun givenPersonName(personident: Personident, either: Either<PdlError, PersonName>) {
        personNameByIdent[personident] = either
    }

    override suspend fun getPersonName(personident: Personident): Either<PdlError, PersonName> {
        return personNameByIdent[personident] ?: Either.Left(GraphQlError("Error when fetching person name"))
    }
}
