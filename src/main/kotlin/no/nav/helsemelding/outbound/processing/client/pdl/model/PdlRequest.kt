package no.nav.helsemelding.outbound.processing.client.pdl.model

import kotlinx.serialization.Serializable

@Serializable
data class PdlRequest(
    val query: String,
    val variables: Variables
)

@Serializable
data class Variables(
    val ident: String,
    val navnHistorikk: Boolean = false
)
