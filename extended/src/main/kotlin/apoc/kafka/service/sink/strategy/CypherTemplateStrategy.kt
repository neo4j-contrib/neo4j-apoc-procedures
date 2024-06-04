package apoc.kafka.service.sink.strategy

import apoc.kafka.service.StreamsSinkEntity
import apoc.kafka.utils.KafkaUtil

class CypherTemplateStrategy(query: String): IngestionStrategy {
    private val fullQuery = "${KafkaUtil.UNWIND} $query"
    override fun mergeNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        return listOf(QueryEvents(fullQuery, events.mapNotNull { it.value as? Map<String, Any> }))
    }

    override fun deleteNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> = emptyList()

    override fun mergeRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> = emptyList()

    override fun deleteRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> = emptyList()

}