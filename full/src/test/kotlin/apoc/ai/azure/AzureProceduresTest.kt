package apoc.ai.azure

//import org.junit.Assert
//import org.junit.ClassRule
//import org.junit.Test
//import org.neo4j.harness.junit.Neo4jRule
//import org.neo4j.harness.junit.rule.Neo4jRule
//
//class AzureProceduresTest {
//    companion object {
//        @JvmStatic val SENTIMENT_URL = ""
//        @JvmStatic val TEXT_KEY = ""
//
//        @ClassRule
//        @JvmField
//        var neo4j = Neo4jRule()
//                .withProcedure(AzureProcedures::class.java)
//    }
//
//    @Test
//    fun `should provide sentiment analysis`() {
//        // given
//        val data = listOf("This is a bad day", "This is a good day")
//
//        // when
//        val response = neo4j.defaultDatabaseService()
//                .executeTransactionally("CALL ai.azure.sentiment(\$url, \$key, \$data)", mapOf("url" to SENTIMENT_URL,
//                        "key" to TEXT_KEY,
//                        "data" to data))
//                .columnAs<Map<String, Any>>("response")
//
//        // then
//        val negative = response.stream().filter { it.getValue("id") == "0" }.findFirst().get()
//        Assert.assertTrue(negative.getValue("score").toString().toDouble() < 0.5 )
//        val positive = response.stream().filter { it.getValue("id") == "1" }.findFirst().get()
//        Assert.assertTrue(positive.getValue("score").toString().toDouble() > 0.5 )
//    }
//}
