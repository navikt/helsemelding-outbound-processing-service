package no.nav.helsemelding.outbound.processing.client.pdl.model

interface PdlClientError {
    val message: String
}

data class PersonNotFound(
    override val message: String
) : PdlClientError

data class QueryError(
    override val message: String,
    val errors: List<PdlError>? = emptyList()
) : PdlClientError

data class FetchingError(
    val code: Int,
    override val message: String
) : PdlClientError
