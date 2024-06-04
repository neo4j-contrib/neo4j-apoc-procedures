package apoc.kafka.extensions

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.selects.whileSelect
import java.util.concurrent.CopyOnWriteArraySet
import java.util.concurrent.TimeoutException


// taken from https://stackoverflow.com/questions/52192752/kotlin-how-to-run-n-coroutines-and-wait-for-first-m-results-or-timeout
@ObsoleteCoroutinesApi
@ExperimentalCoroutinesApi
suspend fun <T> List<Deferred<T>>.awaitAll(timeoutMs: Long): List<T> {
    val jobs = CopyOnWriteArraySet<Deferred<T>>(this)
    val result = ArrayList<T>(size)
    val timeout = ticker(timeoutMs)

    whileSelect {
        jobs.forEach { deferred ->
            deferred.onAwait {
                jobs.remove(deferred)
                result.add(it)
                result.size != size
            }
        }

        timeout.onReceive {
            jobs.forEach { it.cancel() }
            throw TimeoutException("Tasks $size cancelled after timeout of $timeoutMs ms.")
        }
    }

    return result
}

@ExperimentalCoroutinesApi
fun <T> Deferred<T>.errors() = when {
    isCompleted -> getCompletionExceptionOrNull()
    isCancelled -> getCompletionExceptionOrNull() // was getCancellationException()
    isActive -> RuntimeException("Job $this still active")
    else -> null
}