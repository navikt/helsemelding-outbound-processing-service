package no.nav.helsemelding.outbound.processing.client.scopedhttpclient

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
internal data class TokenInfo(
    @SerialName("access_token")
    val accessToken: String,

    @SerialName("expires_in")
    val expiresIn: Int,

    @SerialName("token_type")
    val tokenType: String
)
