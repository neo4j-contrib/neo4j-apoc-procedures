package apoc.kafka.common.utils

import apoc.kafka.utils.KafkaUtil
import org.junit.Test
import java.io.IOException
import kotlin.test.assertNull
import kotlin.test.assertTrue

class StreamsUtilsTest {

    private val foo = "foo"

    @Test
    fun shouldReturnValue() {
        val data = KafkaUtil.ignoreExceptions({
            foo
        }, RuntimeException::class.java)
        assertTrue { data != null && data == foo }
    }

    @Test
    fun shouldIgnoreTheException() {
        val data = KafkaUtil.ignoreExceptions({
            throw RuntimeException()
        }, RuntimeException::class.java)
        assertNull(data)
    }

    @Test(expected = IOException::class)
    fun shouldNotIgnoreTheException() {
        KafkaUtil.ignoreExceptions({
            throw IOException()
        }, RuntimeException::class.java)
    }
}