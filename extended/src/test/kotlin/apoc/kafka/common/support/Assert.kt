package apoc.kafka.common.support

import org.hamcrest.Matcher
import org.hamcrest.StringDescription
import org.neo4j.function.ThrowingSupplier
import java.util.concurrent.TimeUnit

object Assert {
    fun <T, E : java.lang.Exception?> assertEventually(actual: ThrowingSupplier<T, E>, matcher: Matcher<in T>, timeout: Long, timeUnit: TimeUnit) {
        assertEventually({ _: T -> "" }, actual, matcher, timeout, timeUnit)
    }

    fun <T, E : java.lang.Exception?> assertEventually(reason: String, actual: ThrowingSupplier<T, E>, matcher: Matcher<in T>, timeout: Long, timeUnit: TimeUnit) {
        assertEventually({ _: T -> reason }, actual, matcher, timeout, timeUnit)
    }

    fun <T, E : java.lang.Exception?> assertEventually(reason: java.util.function.Function<T, String>, actual: ThrowingSupplier<T, E>, matcher: Matcher<in T>, timeout: Long, timeUnit: TimeUnit) {
        val endTimeMillis = System.currentTimeMillis() + timeUnit.toMillis(timeout)
        while (true) {
            val sampleTime = System.currentTimeMillis()
            val last: T = actual.get()
            val matched: Boolean = matcher.matches(last)
            if (matched || sampleTime > endTimeMillis) {
                if (!matched) {
                    val description = StringDescription()
                    description.appendText(reason.apply(last)).appendText("\nExpected: ").appendDescriptionOf(matcher).appendText("\n     but: ")
                    matcher.describeMismatch(last, description)
                    throw AssertionError("Timeout hit (" + timeout + " " + timeUnit.toString().toLowerCase() + ") while waiting for condition to match: " + description.toString())
                } else {
                    return
                }
            }
            Thread.sleep(100L)
        }
    }

}