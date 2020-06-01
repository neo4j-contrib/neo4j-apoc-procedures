package apoc.nlp.azure

import apoc.util.TestUtil
import org.junit.Assert
import org.junit.Assume
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Test
import org.neo4j.graphdb.Node
import org.neo4j.test.rule.ImpermanentDbmsRule

class AzureProceduresTest {
    companion object {
        @JvmStatic val TEXT_URL = System.getenv("AZURE_TEXT_URL")
        @JvmStatic val TEXT_KEY = System.getenv("AZURE_TEXT_KEY")
        @JvmStatic val VISION_URL = System.getenv("AZURE_VISION_URL")
        @JvmStatic val VISION_KEY = System.getenv("AZURE_VISION_KEY")

        @ClassRule
        @JvmField
        val neo4j = ImpermanentDbmsRule()

        @BeforeClass
        @JvmStatic
        fun beforeClass() {
            TestUtil.registerProcedure(neo4j, AzureProcedures::class.java)
            Assume.assumeTrue((TEXT_URL != null && TEXT_KEY != null) || (VISION_URL != null && VISION_KEY != null))
        }
    }

    @Test
    fun `should provide sentiment analysis`() {
        // given
        val data = listOf("This is a bad day", "This is a good day")

        neo4j.executeTransactionally("CREATE (a:Sentiment{id: 'a', text: 'This is a bad day'}), (b:Sentiment{id: 'b', text: 'This is a good day'})")

        // when
        neo4j.executeTransactionally("""MATCH (s:Sentiment)
            |CALL apoc.nlp.azure.sentiment.stream(s, ${'$'}conf) YIELD node, value, error
            |RETURN *""".trimMargin(),
                mapOf("conf" to mapOf("url" to TEXT_URL, "key" to TEXT_KEY))) {
            // then
            val row1 = it.next()
            val row1Value = row1.getValue("value") as Map<String, Any>
            Assert.assertTrue(row1Value.getValue("score").toString().toDouble() < 0.5)
            val row1Node = row1.getValue("node") as Node
            Assert.assertEquals("a", row1Node.getProperty("id"))

            val row2 = it.next()
            val row2Value = row2.getValue("value") as Map<String, Any>
            Assert.assertTrue(row2Value.getValue("score").toString().toDouble() > 0.5)
            val row2Node = row2.getValue("node") as Node
            Assert.assertEquals("b", row2Node.getProperty("id"))
        }
    }

    @Test
    fun `should provide key phrases analysis`() {
        // given
        val text = """
            "Covid-19 anywhere is Covid-19 everywhere," Melinda Gates said as she called for global co-operation to beat the pandemic.
            The philanthropist was speaking to Emma Barnett on BBC Radio 5 Live after President Donald Trump announced the US would stop funding the World Health Organization (WHO).
            The Bill and Melinda Gates Foundation - the second-largest funder of the WHO - has pledged a further ${'$'}150m (£120m) to fight Covid-19, but she said they did not expect a vaccine to be available for 18 months.
        """.trimIndent()
        neo4j.executeTransactionally("CREATE (a:KeyPhrase{id: 'a', text: ${'$'}data})",
                mapOf("data" to text))

        // when
        neo4j.executeTransactionally("""MATCH (s:KeyPhrase)
            |CALL apoc.nlp.azure.keyPhrases.stream(s, ${'$'}conf) YIELD node, value, error
            |RETURN *""".trimMargin(),
                mapOf("conf" to mapOf("url" to TEXT_URL, "key" to TEXT_KEY))) {

            // then
            val row = it.next()
            val map = row["value"] as Map<String, Any>
            val expected = listOf("Covid", "Melinda Gates Foundation", "BBC Radio",
                    "President Donald Trump", "US", "Emma Barnett", "Bill",
                    "World Health Organization", "largest funder",
                    "vaccine", "months", "philanthropist", "pandemic")
            Assert.assertEquals(expected, map["keyPhrases"])
            val node = row["node"] as Node
            Assert.assertEquals("a", node.getProperty("id"))
        }
    }

    @Test
    fun `should provide entity analysis`() {
        // given
        val text = "I had a wonderful trip to Seattle last week."
        neo4j.executeTransactionally("CREATE (a:Entity{id: 'a', text: ${'$'}data})",
                mapOf("data" to text))

        // when
        neo4j.executeTransactionally("""MATCH (s:Entity)
            |CALL apoc.nlp.azure.entities.stream(s, ${'$'}conf) YIELD node, value, error
            |RETURN *""".trimMargin(),
                mapOf("conf" to mapOf("url" to TEXT_URL, "key" to TEXT_KEY))) {

            // then
            val row = it.next()
            val map = row["value"] as Map<String, Any>
            val entities = map["entities"] as List<Map<String, Any>>
            val resultEntities = entities.map { it["name"] as String }
            val expected = listOf("Seattle", "last week")
            Assert.assertEquals(expected, resultEntities)
        }
    }

//    @Test
//    fun `should provide computer vision analysis`() {
//        // given
//        val data = "https://staticfanpage.akamaized.net/wp-content/uploads/sites/12/2018/10/640px-torre_di_pisa_vista_dal_cortile_dellopera_del_duomo_06-638x425.jpg"
//
//        // when
//        neo4j.executeTransactionally("CALL apoc.ai.azure.vision(\$url, \$key, \$data)",
//                mapOf("url" to VISION_URL, "key" to VISION_KEY, "data" to data)) {
//
//            // then
//            val response = it.columnAs<Map<String, Any>>("value")
//            val map = response.stream().findFirst().get()
//            val categories = map["categories"] as List<Map<String, Any>>
//            val resultEntities = categories.map { it["name"] as String }
//            val expected = listOf("building_")
//            Assert.assertEquals(expected, resultEntities)
//        }
//    }
}