package no.nav.helsemelding.outbound.processing.client.pdl.model

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class PersonResponse(
    val errors: List<GraphQlResponseError> = emptyList(),
    val data: PersonData? = null
)

@Serializable
data class GraphQlResponseError(
    val message: String,
    val locations: List<ErrorLocation> = emptyList(),
    val path: List<String> = emptyList(),
    val extensions: ErrorExtension = ErrorExtension()
)

@Serializable
data class ErrorLocation(
    val line: Int?,
    val column: Int?
)

@Serializable
data class ErrorExtension(
    val code: String? = null,
    val classification: String? = null
)

@Serializable
data class PersonData(
    @SerialName("hentPerson")
    val person: Person?
)

@Serializable
data class Person(
    @SerialName("navn")
    val names: List<PersonName>
)

@Serializable
data class PersonName(
    @SerialName("fornavn")
    val firstName: String,

    @SerialName("mellomnavn")
    val middleName: String?,

    @SerialName("etternavn")
    val lastName: String
)

fun GraphQlResponseError.errorMessage(): String =
    "$message with code: ${extensions.code} and classification: ${extensions.classification}"
