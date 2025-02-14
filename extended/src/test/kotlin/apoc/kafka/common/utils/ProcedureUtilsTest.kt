package apoc.kafka.common.utils

import apoc.kafka.utils.KafkaUtil
import org.junit.ClassRule
import org.junit.Test
import org.neo4j.test.rule.ImpermanentDbmsRule
import kotlin.test.assertFalse

class ProcedureUtilsTest {

    companion object {
        @ClassRule @JvmField
        val db = ImpermanentDbmsRule()
    }

    @Test
    fun shouldCheckIfIsACluster() {
        val isCluster = KafkaUtil.isCluster(db)
        assertFalse { isCluster }
    }

}