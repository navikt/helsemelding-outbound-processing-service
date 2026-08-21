package no.nav.helsemelding.outbound.processing.client.pdl.model

import no.nav.helsemelding.outbound.processing.client.ClientError
import no.nav.helsemelding.outbound.processing.model.ErrorCode.PDL_ERROR

sealed interface PdlError : ClientError {
    override val code get() = PDL_ERROR
}

data class PersonNotFound(
    override val message: String
) : PdlError

data class GraphQlError(
    override val message: String,
    val errors: List<GraphQlResponseError> = emptyList()
) : PdlError

data class HttpError(
    val statusCode: Int,
    override val message: String
) : PdlError

data class UnexpectedError(
    override val message: String,
    val cause: Throwable
) : PdlError
