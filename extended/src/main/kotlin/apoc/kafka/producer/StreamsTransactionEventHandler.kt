package apoc.kafka.producer

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Transaction
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.neo4j.graphdb.event.TransactionData
import org.neo4j.graphdb.event.TransactionEventListener
import org.neo4j.kernel.internal.GraphDatabaseAPI
import apoc.kafka.events.*
import apoc.kafka.extensions.labelNames
import apoc.kafka.extensions.registerTransactionEventListener
import apoc.kafka.extensions.unregisterTransactionEventListener
import apoc.kafka.producer.events.*
import apoc.kafka.utils.KafkaUtil.getNodeKeys
import java.net.InetAddress
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference


class StreamsTransactionEventHandler(private val router: StreamsEventRouter,
                                     private val db: GraphDatabaseAPI,
                                     private val streamsConstraintsService: StreamsConstraintsService
)
    : TransactionEventListener<PreviousTransactionData> {

    private val status = AtomicReference(StreamsPluginStatus.UNKNOWN)

    fun start() {
        db.registerTransactionEventListener(this)
        status.set(StreamsPluginStatus.RUNNING)
    }

    fun stop() {
        db.unregisterTransactionEventListener(this)
        status.set(StreamsPluginStatus.STOPPED)
    }

    fun status() = status.get()

    private val configuration = router.eventRouterConfiguration

    private val nodeRoutingLabels = configuration.nodeRouting
            .flatMap { it.labels }
    private val relRoutingTypesAndStrategies = configuration.relRouting
            .map { it.name to it.relKeyStrategy }.toMap()

    private val nodeAll = configuration.nodeRouting.any { it.labels.isEmpty() }
    private val relAll = configuration.relRouting.any { it.name.isNullOrBlank() }

    // As getting host name in some network configuration can be expensive
    // this can lead to slowness in the start-up process (i.e. slowing the leader 
    // election in case of a Causal Cluster). We define it a `lazy` value
    // computing it at the first invocation
    private val hostName by lazy { InetAddress.getLocalHost().hostName }

    /**
     * Wrap the payload into a StreamsTransactionEvent for the eventId
     */
    private fun payloadToEvent(operation: OperationType, payload: Payload, schema: Schema, txd: TransactionData, eventId: Int, eventCount: Int) : StreamsTransactionEvent{
        val meta = StreamsEventMetaBuilder()
                .withOperation(operation)
                .withTransactionEventId(eventId)
                .withTransactionEventsCount(eventCount)
                .withUsername(txd.username())
                .withTimestamp(txd.commitTime)
                .withTransactionId(txd.transactionId)
                .withHostname(hostName)
                .build()

        val builder = StreamsTransactionEventBuilder()
                .withMeta(meta)
                .withPayload(payload)
                .withSchema(schema)

        return builder.build()
    }

    private fun mapToStreamsEvent(operation: OperationType, payloads: List<Payload>, txd: TransactionData, totalEventsCount: Int, accumulator: AtomicInteger,
            nodeConstraints: Map<String, Set<Constraint>>, relConstraints: Map<String, Set<Constraint>>) : List<StreamsTransactionEvent> {

        val getNodeConstraintsByLabels: (Collection<String>?) -> Set<Constraint> = { labels ->
            labels.orEmpty()
                    .flatMap { label -> nodeConstraints[label].orEmpty() }
                    .toSet()
        }

        return payloads.map { payload ->
            accumulator.incrementAndGet()
            val schema = if (payload is NodePayload) {
                val constraints = getNodeConstraintsByLabels((payload.after ?: payload.before)!!.labels)
                SchemaBuilder()
                        .withPayload(payload)
                        .withConstraints(constraints)
                        .build()
            } else  {
                val relationshipPayload = (payload as RelationshipPayload)
                val relType = relationshipPayload.label
                val constraints = (relConstraints[relType].orEmpty()
                        + getNodeConstraintsByLabels(relationshipPayload.start.labels)
                        + getNodeConstraintsByLabels(relationshipPayload.end.labels))
                SchemaBuilder()
                        .withPayload(payload)
                        .withConstraints(constraints)
                        .build()
            }
            payloadToEvent(operation, payload, schema, txd, accumulator.get(), totalEventsCount)
        }
    }

    private fun <T> allOrFiltered(iterable: Iterable<T>,
                                  all: Boolean,
                                  predicate: (T) -> Boolean): Iterable<T> = when (all) {
        true -> iterable
        else -> iterable.filter(predicate)
    }

    private fun buildNodeChanges(txd: TransactionData, builder: PreviousTransactionDataBuilder): PreviousTransactionDataBuilder {
        val createdPayload = allOrFiltered(txd.createdNodes(), nodeAll)
                { it.labelNames().any { nodeRoutingLabels.contains(it) } }
                .map {
                    val labels = it.labelNames()

                    val afterNode = NodeChangeBuilder()
                            .withLabels(labels)
                            .withProperties(it.allProperties)
                            .build()

                    val payload = NodePayloadBuilder()
                            .withId(it.id.toString())
                            .withAfter(afterNode)
                            .build()

                    it.id.toString() to payload
                }
                .toMap()

        // returns a Map<Boolean, List<Node>> where the K is true if the node has been deleted
        val removedNodeProps = txd.removedNodeProperties()
                .map { txd.deletedNodes().contains(it.entity()) to it }
                .groupBy({ it.first }, { it.second })
                .toMap()
        val removedLbls = txd.removedLabels()
                .map { txd.deletedNodes().contains(it.node()) to it }
                .groupBy({ it.first }, { it.second })
                .toMap()

        // labels and properties of deleted nodes are unreachable
        val deletedNodeProperties = removedNodeProps.getOrDefault(true, emptyList())
                .map { it.entity().id to (it.key() to it.previouslyCommittedValue()) }
                .groupBy({ it.first },{ it.second }) // { nodeId -> [(k,v)] }
                .mapValues { it.value.toMap() }

        val deletedLabels = removedLbls.getOrDefault(true, emptyList())
                .map { labelEntry -> labelEntry.node().id to labelEntry.label().name() } // [ (nodeId, [label]) ]
                .groupBy({it.first},{it.second}) // { nodeId -> [label]  }

        val removedNodeProperties = removedNodeProps.getOrDefault(false, emptyList())
        val removedLabels = removedLbls.getOrDefault(false, emptyList())

        val deletedPayload = txd.deletedNodes()
                .map {
                    val beforeNode = NodeChangeBuilder()
                            .withLabels(deletedLabels.getOrDefault(it.id, emptyList()))
                            .withProperties(deletedNodeProperties.getOrDefault(it.id, emptyMap()))
                            .build()

                    val payload = NodePayloadBuilder()
                            .withId(it.id.toString())
                            .withBefore(beforeNode)
                            .build()

                    it.id.toString() to payload
                }
                .toMap()

        //don't change the order of the with methods
        return builder.withLabels(txd.assignedLabels(),removedLabels)
                .withNodeProperties(txd.assignedNodeProperties(),removedNodeProperties)
                .withNodeCreatedPayloads(createdPayload)
                .withNodeDeletedPayloads(deletedPayload)
                .withDeletedLabels(deletedLabels)
    }

    private fun buildRelationshipChanges(txd: TransactionData, builder: PreviousTransactionDataBuilder, nodeConstraints: Map<String, Set<Constraint>>): PreviousTransactionDataBuilder {
        // returns a Map<Boolean, List<Relationship>> where the K is true if the node has been deleted
        val removeRelProps = allOrFiltered(txd.removedRelationshipProperties(), relAll)
                { relRoutingTypesAndStrategies.containsKey(it.entity().type.name()) }
                .map { txd.deletedRelationships().contains(it.entity()) to it }
                .groupBy({ it.first }, { it.second })
                .toMap()

        val deletedRelProperties = removeRelProps.getOrDefault(true, emptyList())
                .map { it.entity().id to (it.key() to it.previouslyCommittedValue()) }
                .groupBy({ it.first }, { it.second }) // { nodeId -> [(k,v)] }
                .mapValues { it.value.toMap() }

        val nodeConstraintsCache = mutableMapOf<List<String>, List<Constraint>>()
        val filterNodeConstraintCache : (List<String>) -> List<Constraint> = { startLabels ->
            nodeConstraintsCache.computeIfAbsent(startLabels) {
                nodeConstraints
                        .filterKeys { startLabels.contains(it) }
                        .values
                        .flatten()
            }
        }

        val createdRelPayload = allOrFiltered(txd.createdRelationships(), relAll)
                { relRoutingTypesAndStrategies.containsKey(it.type.name()) }
                .map {
                    val afterRel = RelationshipChangeBuilder()
                            .withProperties(it.allProperties)
                            .build()

                    val relKeyStrategy = relRoutingTypesAndStrategies.getOrDefault(it.type.name(), RelKeyStrategy.DEFAULT)

                    val startLabels = it.startNode.labelNames()
                    val startNodeConstraints = filterNodeConstraintCache(startLabels)
                    val startKeys = getNodeKeys(startLabels, it.startNode.propertyKeys.toSet(), startNodeConstraints, relKeyStrategy)
                            .toTypedArray()

                    val endLabels = it.endNode.labelNames()
                    val endNodeConstraints = filterNodeConstraintCache(endLabels)
                    val endKeys = getNodeKeys(endLabels, it.endNode.propertyKeys.toSet(), endNodeConstraints, relKeyStrategy)
                            .toTypedArray()

                    val payload = RelationshipPayloadBuilder()
                            .withId(it.id.toString())
                            .withName(it.type.name())
                            .withStartNode(it.startNode.id.toString(), startLabels, it.startNode.getProperties(*startKeys))
                            .withEndNode(it.endNode.id.toString(), endLabels, it.endNode.getProperties(*endKeys))
                            .withAfter(afterRel)
                            .build()

                    it.id.toString() to payload
                }
                .toMap()

        val deletedRelPayload = allOrFiltered(txd.deletedRelationships(), relAll)
                { relRoutingTypesAndStrategies.containsKey(it.type.name()) }
                .map {
                    val beforeRel = RelationshipChangeBuilder()
                            .withProperties(deletedRelProperties.getOrDefault(it.id, emptyMap()))
                            .build()

                    // start and end can be unreachable in case of detach delete
                    val isStartNodeDeleted = txd.isDeleted(it.startNode)
                    val isEndNodeDeleted = txd.isDeleted(it.endNode)

                    val startNodeLabels = if (isStartNodeDeleted) builder.deletedLabels(it.startNode.id) else it.startNode.labelNames()
                    val endNodeLabels = if (isEndNodeDeleted) builder.deletedLabels(it.endNode.id) else it.endNode.labelNames()

                    val startPropertyKeys = if (isStartNodeDeleted) {
                        builder.nodeDeletedPayload(it.startNodeId)?.before?.properties?.keys.orEmpty()
                    } else {
                        it.startNode.propertyKeys
                    }

                    val endPropertyKeys = if (isEndNodeDeleted) {
                        builder.nodeDeletedPayload(it.endNodeId)?.before?.properties?.keys.orEmpty()
                    } else {
                        it.endNode.propertyKeys
                    }
                    val relKeyStrategy = relRoutingTypesAndStrategies.getOrDefault(it.type.name(), RelKeyStrategy.DEFAULT)

                    val startNodeConstraints = filterNodeConstraintCache(startNodeLabels)
                    val startKeys = getNodeKeys(startNodeLabels, startPropertyKeys.toSet(), startNodeConstraints, relKeyStrategy)

                    val endNodeConstraints = filterNodeConstraintCache(endNodeLabels)
                    val endKeys = getNodeKeys(endNodeLabels, endPropertyKeys.toSet(), endNodeConstraints, relKeyStrategy)

                    val startProperties = if (isStartNodeDeleted) {
                        val payload = builder.nodeDeletedPayload(it.startNode.id)!!
                        (payload.after ?: payload.before)?.properties?.filterKeys { startKeys.contains(it) }.orEmpty()
                    } else {
                        it.startNode.getProperties(*startKeys.toTypedArray())
                    }
                    val endProperties = if (isEndNodeDeleted) {
                        val payload = builder.nodeDeletedPayload(it.endNode.id)!!
                        (payload.after ?: payload.before)?.properties?.filterKeys { endKeys.contains(it) }.orEmpty()
                    } else {
                        it.endNode.getProperties(*endKeys.toTypedArray())
                    }

                    val payload = RelationshipPayloadBuilder()
                            .withId(it.id.toString())
                            .withName(it.type.name())
                            .withStartNode(it.startNode.id.toString(), startNodeLabels, startProperties)
                            .withEndNode(it.endNode.id.toString(), endNodeLabels, endProperties)
                            .withBefore(beforeRel)
                            .build()

                    it.id.toString() to payload
                }
                .toMap()

        val removedRelsProperties = removeRelProps.getOrDefault(false, emptyList())

        //don't change the order of the with methods
        return builder.withRelProperties(txd.assignedRelationshipProperties(), removedRelsProperties)
                .withRelCreatedPayloads(createdRelPayload)
                .withRelDeletedPayloads(deletedRelPayload)
                .withRelRoutingTypesAndStrategies(relRoutingTypesAndStrategies)
    }

    override fun afterRollback(p0: TransactionData?, p1: PreviousTransactionData?, db: GraphDatabaseService?) {}

    override fun afterCommit(txd: TransactionData, previousTxd: PreviousTransactionData, db: GraphDatabaseService?) = runBlocking {
        val nodePrevious = previousTxd.nodeData
        val relPrevious = previousTxd.relData

        val totalEventsCount = nodePrevious.createdPayload.size + nodePrevious.deletedPayload.size + nodePrevious.updatedPayloads.size +
                relPrevious.createdPayload.size + relPrevious.deletedPayload.size + relPrevious.updatedPayloads.size

        if (totalEventsCount == 0) {
            return@runBlocking
        }

        val eventAcc = AtomicInteger(-1)
        val events = mutableListOf<StreamsTransactionEvent>()
        val nodeCreated = async { mapToStreamsEvent(OperationType.created, nodePrevious.createdPayload, txd, totalEventsCount, eventAcc,
                previousTxd.nodeConstraints, previousTxd.relConstraints) }
        val nodeDeleted = async { mapToStreamsEvent(OperationType.deleted, nodePrevious.deletedPayload, txd, totalEventsCount, eventAcc,
                previousTxd.nodeConstraints, previousTxd.relConstraints) }
        val nodeUpdated = async { mapToStreamsEvent(OperationType.updated, nodePrevious.updatedPayloads, txd, totalEventsCount, eventAcc,
                previousTxd.nodeConstraints, previousTxd.relConstraints) }
        val relCreated = async { mapToStreamsEvent(OperationType.created, relPrevious.createdPayload, txd, totalEventsCount, eventAcc,
                previousTxd.nodeConstraints, previousTxd.relConstraints) }
        val relDeleted = async { mapToStreamsEvent(OperationType.deleted, relPrevious.deletedPayload, txd, totalEventsCount, eventAcc,
                previousTxd.nodeConstraints, previousTxd.relConstraints) }
        val relUpdated = async { mapToStreamsEvent(OperationType.updated, relPrevious.updatedPayloads, txd, totalEventsCount, eventAcc,
                previousTxd.nodeConstraints, previousTxd.relConstraints) }
        events.addAll(nodeCreated.await())
        events.addAll(nodeDeleted.await())
        events.addAll(nodeUpdated.await())
        events.addAll(relCreated.await())
        events.addAll(relDeleted.await())
        events.addAll(relUpdated.await())

        val topicEventsMap = events.flatMap { event ->
                    val map  = when (event.payload.type) {
                        EntityType.node -> NodeRoutingConfiguration.prepareEvent(event, configuration.nodeRouting)
                        EntityType.relationship -> RelationshipRoutingConfiguration.prepareEvent(event, configuration.relRouting)
                    }
                    map.entries
                }
                .groupBy({ it.key }, { it.value })

        topicEventsMap.forEach {
            router.sendEvents(it.key, it.value)
        }
    }

    override fun beforeCommit(txd: TransactionData, tx: Transaction?, db: GraphDatabaseService?): PreviousTransactionData {
        val nodeConstraints = streamsConstraintsService.allForLabels()
        val relConstraints = streamsConstraintsService.allForRelationshipType()
        var builder = PreviousTransactionDataBuilder()
                .withNodeConstraints(nodeConstraints)
                .withRelConstraints(relConstraints)

        builder = buildNodeChanges(txd, builder)
        builder = buildRelationshipChanges(txd, builder, nodeConstraints)

        return builder.build()
    }
}
