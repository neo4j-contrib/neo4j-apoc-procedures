package apoc.kafka.extensions

import apoc.kafka.utils.KafkaUtil
import org.neo4j.common.DependencyResolver
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.event.TransactionEventListener
import org.neo4j.kernel.internal.GraphDatabaseAPI

fun GraphDatabaseService.execute(cypher: String) = this.execute(cypher, emptyMap())
fun GraphDatabaseService.execute(cypher: String, params: Map<String, Any>) = this.executeTransactionally(cypher, params)

fun <T> GraphDatabaseService.execute(cypher: String, lambda: ((Result) -> T)) = this.execute(cypher, emptyMap(), lambda)
fun <T> GraphDatabaseService.execute(cypher: String,
                                     params: Map<String, Any>,
                                     lambda: ((Result) -> T)) = this.executeTransactionally(cypher, params, lambda)
