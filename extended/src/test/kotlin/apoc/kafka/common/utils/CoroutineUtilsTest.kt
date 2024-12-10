package apoc.kafka.common.utils

import apoc.kafka.utils.KafkaUtil.retryForException
import kotlinx.coroutines.runBlocking
import org.junit.Test
import java.io.IOException
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CoroutineUtilsTest {

    @Test
    fun `should success after retry for known exception`() = runBlocking {
        var count = 0
        var excuted = false
        retryForException(exceptions = arrayOf(RuntimeException::class.java),
                retries = 4, delayTime = 100) {
            if (count < 2) {
                ++count
                throw RuntimeException()
            }
            excuted = true
        }

        assertEquals(2, count)
        assertTrue { excuted }
    }

    @Test(expected = RuntimeException::class)
    fun `should fail after retry for known exception`() {
        var retries = 3
        runBlocking {
            retryForException(exceptions = arrayOf(RuntimeException::class.java),
                    retries = 3, delayTime = 100) {
                if (retries >= 0) {
                    --retries
                    throw RuntimeException()
                }
            }
        }
    }

    @Test
    fun `should fail fast unknown exception`() {
        var iteration = 0
        var isIOException = false
        try {
            runBlocking {
                retryForException(exceptions = arrayOf(RuntimeException::class.java),
                        retries = 3, delayTime = 100) {
                    if (iteration >= 0) {
                        ++iteration
                        throw IOException()
                    }
                }
            }
        } catch (e: Exception) {
            isIOException = e is IOException
        }
        assertTrue { isIOException }
        assertEquals(1, iteration)
    }
}