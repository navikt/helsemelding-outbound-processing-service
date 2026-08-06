package no.nav.helsemelding.outbound.processing.client.pdl.model

sealed interface ClientError {
    val message: String
}

data class PersonNotFound(
    override val message: String
) : ClientError

data class GraphQlError(
    override val message: String,
    val errors: List<GraphQlResponseError> = emptyList()
) : ClientError

data class HttpError(
    val statusCode: Int,
    override val message: String
) : ClientError

data class UnexpectedClientError(
    override val message: String,
    val cause: Throwable
) : ClientError
