//package apoc.kafka
//
//import apoc.kafka.consumer.StreamsSinkConfiguration
//import apoc.kafka.consumer.StreamsTopicService
//import apoc.kafka.extensions.isDefaultDb
//import apoc.kafka.service.StreamsStrategyStorage
//import apoc.kafka.service.TopicType
//import apoc.kafka.service.sink.strategy.*
//import org.neo4j.graphdb.GraphDatabaseService
//
//class Neo4jStreamsStrategyStorage(private val streamsTopicService: StreamsTopicService,
//                                  private val streamsConfig: Map<String, String>,
//                                  private val db: GraphDatabaseService): StreamsStrategyStorage() {
//
//    override fun getTopicType(topic: String): TopicType? {
//        return streamsTopicService.getTopicType(topic)
//    }
//
//    private fun <T> getTopicsByTopicType(topicType: TopicType): T = streamsTopicService.getByTopicType(topicType) as T
//
//    override fun getStrategy(topic: String): IngestionStrategy = when (val topicType = getTopicType(topic)) {
//        TopicType.CDC_SOURCE_ID -> {
//            val strategyConfig = StreamsSinkConfiguration
//                    .createSourceIdIngestionStrategyConfig(streamsConfig, db.databaseName(), db.isDefaultDb())
//            SourceIdIngestionStrategy(strategyConfig)
//        }
//        TopicType.CDC_SCHEMA -> SchemaIngestionStrategy()
//        TopicType.CUD -> CUDIngestionStrategy()
//        TopicType.PATTERN_NODE -> {
//            val map = getTopicsByTopicType<Map<String, NodePatternConfiguration>>(topicType)
//            NodePatternIngestionStrategy(map.getValue(topic))
//        }
//        TopicType.PATTERN_RELATIONSHIP -> {
//            val map = getTopicsByTopicType<Map<String, RelationshipPatternConfiguration>>(topicType)
//            RelationshipPatternIngestionStrategy(map.getValue(topic))
//        }
//        TopicType.CYPHER -> {
//            CypherTemplateStrategy(streamsTopicService.getCypherTemplate(topic)!!)
//        }
//        else -> throw RuntimeException("Topic Type not Found")
//    }
//
//}