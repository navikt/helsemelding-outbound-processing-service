package no.nav.helsemelding.outbound.processing.client.pdl

import arrow.core.Either
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.client.statement.request
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import no.nav.helsemelding.message.msghead.model.Personident
import no.nav.helsemelding.outbound.processing.client.pdl.model.FetchingError
import no.nav.helsemelding.outbound.processing.client.pdl.model.PdlClientError
import no.nav.helsemelding.outbound.processing.client.pdl.model.PdlPersonNavn
import no.nav.helsemelding.outbound.processing.client.pdl.model.PdlPersonResponse
import no.nav.helsemelding.outbound.processing.client.pdl.model.PdlRequest
import no.nav.helsemelding.outbound.processing.client.pdl.model.PersonNotFound
import no.nav.helsemelding.outbound.processing.client.pdl.model.QueryError
import no.nav.helsemelding.outbound.processing.client.pdl.model.Variables
import no.nav.helsemelding.outbound.processing.client.pdl.model.errorMessage

private val log = KotlinLogging.logger {}

class HttpPdlClient(
    clientProvider: () -> HttpClient,
    private val pdlServiceUrl: String
) : PdlClient {

    private val httpClient = clientProvider()

    override suspend fun getPersonName(personident: Personident): Either<PdlClientError, PdlPersonNavn> {
        val request = PdlRequest(QUERY, Variables(personident.value))

        val response: HttpResponse = httpClient.post(pdlServiceUrl) {
            contentType(ContentType.Application.Json)
            header(BEHANDLINGSNUMMER_HEADER_KEY, BEHANDLINGSNUMMER_HEADER_VALUE)
            setBody(request)
        }.withLogging()

        when (response.status) {
            HttpStatusCode.OK -> {
                val pdlPersonReponse = response.body<PdlPersonResponse>()
                return if (!pdlPersonReponse.errors.isNullOrEmpty()) {
                    val errorMessage = "Error while requesting person from PersonDataLosningen"
                    pdlPersonReponse.errors.forEach {
                        log.error { "$errorMessage: ${it.errorMessage()}" }
                    }
                    Either.Left(QueryError(errorMessage, pdlPersonReponse.errors))
                } else {
                    when (val pdlPerson = pdlPersonReponse.data?.hentPerson) {
                        null -> Either.Left(PersonNotFound("Person not found: data or hentPerson is null"))
                        else -> when (pdlPerson.navn.isEmpty()) {
                            true -> Either.Left(PersonNotFound("Person not found: navn empty"))
                            false -> Either.Right(pdlPerson.navn.first())
                        }
                    }
                }
            }

            else -> {
                log.error { "Request with url: $pdlServiceUrl failed with response code: ${response.status.value}" }
                return Either.Left(response.toFetchingError())
            }
        }
    }

    companion object {
        val QUERY = """
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

        const val BEHANDLINGSNUMMER_HEADER_KEY = "behandlingsnummer"
        const val BEHANDLINGSNUMMER_HEADER_VALUE = ""
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

interface PdlClient {
    suspend fun getPersonName(personident: Personident): Either<PdlClientError, PdlPersonNavn>
}

class FakePdlClient : PdlClient {
    private val personNameByIdent = mutableMapOf<Personident, Either<PdlClientError, PdlPersonNavn>>()

    fun givenPersonName(personident: Personident, either: Either<PdlClientError, PdlPersonNavn>) {
        personNameByIdent[personident] = either
    }

    override suspend fun getPersonName(personident: Personident): Either<PdlClientError, PdlPersonNavn> {
        return personNameByIdent[personident] ?: Either.Left(QueryError("Error when fetching person name"))
    }
}
