package no.nav.helsemelding.outbound.processing.client.pdl.model

import kotlinx.serialization.Serializable

@Serializable
data class PdlPersonResponse(
    val errors: List<PdlError>?,
    val data: PdlHentPerson?
)

@Serializable
data class PdlError(
    val message: String,
    val locations: List<PdlErrorLocation>,
    val path: List<String>?,
    val extensions: PdlErrorExtension
) {
    fun isNotFound() = this.extensions.code == "not_found"
}

@Serializable
data class PdlErrorLocation(
    val line: Int?,
    val column: Int?
)

@Serializable
data class PdlErrorExtension(
    val code: String?,
    val classification: String
)

@Serializable
data class PdlHentPerson(
    val hentPerson: PdlPerson?
)

@Serializable
data class PdlPerson(
    val navn: List<PdlPersonNavn>
)

@Serializable
data class PdlPersonNavn(
    val fornavn: String,
    val mellomnavn: String?,
    val etternavn: String
)

fun PdlError.errorMessage(): String {
    return "${this.message} with code: ${extensions.code} and classification: ${extensions.classification}"
}
