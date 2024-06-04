package apoc.kafka.service.sink.strategy

import apoc.kafka.events.Constraint
import apoc.kafka.events.RelationshipPayload
import apoc.kafka.service.StreamsSinkEntity


data class QueryEvents(val query: String, val events: List<Map<String, Any?>>)

interface IngestionStrategy {
    fun mergeNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents>
    fun deleteNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents>
    fun mergeRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents>
    fun deleteRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents>
}

data class RelationshipSchemaMetadata(val label: String,
                                      val startLabels: List<String>,
                                      val endLabels: List<String>,
                                      val startKeys: Set<String>,
                                      val endKeys: Set<String>) {
    constructor(payload: RelationshipPayload) : this(label = payload.label,
            startLabels = payload.start.labels.orEmpty(),
            endLabels = payload.end.labels.orEmpty(),
            startKeys = payload.start.ids.keys,
            endKeys = payload.end.ids.keys)
}

data class NodeSchemaMetadata(val constraints: List<Constraint>,
                              val labelsToAdd: List<String>,
                              val labelsToDelete: List<String>,
                              val keys: Set<String>)



data class NodeMergeMetadata(val labelsToAdd: Set<String>,
                             val labelsToDelete: Set<String>)