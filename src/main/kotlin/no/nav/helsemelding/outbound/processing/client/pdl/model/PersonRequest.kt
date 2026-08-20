package no.nav.helsemelding.outbound.processing.client.pdl.model

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class PersonRequest(
    val query: String,
    val variables: PersonVariables
)

@Serializable
data class PersonVariables(
    @SerialName("ident")
    val nationalIdentityNumber: String,

    @SerialName("navnHistorikk")
    val includeNameHistory: Boolean
)
