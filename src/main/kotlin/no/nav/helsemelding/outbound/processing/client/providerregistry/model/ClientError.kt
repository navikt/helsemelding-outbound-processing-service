package no.nav.helsemelding.outbound.processing.client.providerregistry.model

sealed interface ClientError {
    val message: String
}

data class HttpError(
    val statusCode: Int,
    override val message: String
) : ClientError

data class UnexpectedClientError(
    override val message: String,
    val cause: Throwable
) : ClientError
