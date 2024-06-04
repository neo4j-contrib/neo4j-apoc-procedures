package apoc.kafka.common.utils

import apoc.kafka.utils.KafkaUtil
import org.junit.ClassRule
import org.junit.Test
import org.neo4j.test.rule.ImpermanentDbmsRule
import kotlin.test.assertTrue

class Neo4jUtilsTest {

    companion object {
        @ClassRule @JvmField
        val db = ImpermanentDbmsRule()
    }

    @Test
    fun shouldCheckIfIsWriteableInstance() {
        val isWriteableInstance = KafkaUtil.isWriteableInstance(db)
        assertTrue { isWriteableInstance }
    }
}