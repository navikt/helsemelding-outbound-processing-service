package no.nav.helsemelding.outbound.processing.util

import arrow.fx.coroutines.ExitCase
import arrow.fx.coroutines.ResourceScope
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext

suspend fun ResourceScope.coroutineScope(): CoroutineScope {
    val context = currentCoroutineContext()
    val job = context[Job]?.let { Job(it) } ?: Job()
    return install({ CoroutineScope(context + job) }) { _, exitCase ->
        when (exitCase) {
            ExitCase.Completed -> job.cancel()
            is ExitCase.Cancelled -> job.cancel(exitCase.exception)
            is ExitCase.Failure -> job.cancel("Resource failed, so cancelling associated scope", exitCase.failure)
        }
        job.join()
    }
}
