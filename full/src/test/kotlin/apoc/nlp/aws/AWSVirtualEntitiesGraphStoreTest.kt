package apoc.nlp.aws

import apoc.nlp.NodeMatcher
import apoc.nlp.RelationshipMatcher
import apoc.result.VirtualNode
import com.amazonaws.services.comprehend.model.BatchDetectEntitiesItemResult
import com.amazonaws.services.comprehend.model.BatchDetectEntitiesResult
import com.amazonaws.services.comprehend.model.BatchItemError
import com.amazonaws.services.comprehend.model.Entity
import junit.framework.Assert.assertEquals
import junit.framework.Assert.assertNotNull
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.hasItem
import org.junit.ClassRule
import org.junit.Test
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.test.rule.ImpermanentDbmsRule
import java.util.stream.Collectors
import org.neo4j.configuration.SettingImpl.newBuilder;
import org.neo4j.configuration.SettingValueParsers.BOOL;

class AWSVirtualEntitiesGraphStoreTest {
    companion object {
        @ClassRule
        @JvmField
        val neo4j = ImpermanentDbmsRule()
                .withSetting(newBuilder( "unsupported.dbms.debug.track_cursor_close", BOOL, false ).build(), false)
                .withSetting(newBuilder( "unsupported.dbms.debug.trace_cursors", BOOL, false ).build(), false)

    }

    @Test
    fun `store graph from result with multiple source nodes with overlapping entities`() {
        neo4j.beginTx().use {
            val itemResult1 = BatchDetectEntitiesItemResult().withEntities(
                    Entity().withText("The Matrix").withType("Movie").withScore(0.9F),
                    Entity().withText("The Notebook").withType("Movie").withScore(0.9F))
                    .withIndex(0)

            val itemResult2 = BatchDetectEntitiesItemResult().withEntities(
                    Entity().withText("The Matrix").withType("Movie").withScore(0.9F),
                    Entity().withText("Titanic").withType("Movie").withScore(0.9F),
                    Entity().withText("Top Boy").withType("Television").withScore(0.9F))
                    .withIndex(1)

            val res = BatchDetectEntitiesResult().withErrorList(BatchItemError()).withResultList(itemResult1, itemResult2)
            val sourceNode1 = it.createNode(Label {"Person"})
            sourceNode1.setProperty("id", 1234L)
            val sourceNode2 = it.createNode(Label {"Person"})
            sourceNode2.setProperty("id", 5678L)

            val virtualGraph = AWSVirtualEntitiesGraph(res, listOf(sourceNode1, sourceNode2), RelationshipType { "ENTITY" }, "score", 0.0).createAndStore(it)

            // verify virtual graph
            val nodes = virtualGraph.graph["nodes"] as Set<*>
            assertEquals(6, nodes.size)
            assertThat(nodes, hasItem(sourceNode1))
            assertThat(nodes, hasItem(sourceNode2))

            val matrixNode = VirtualNode(arrayOf(Label{"Movie"}, Label{"Entity"}), mapOf("text" to "The Matrix", "type" to "Movie"))
            val notebookNode = VirtualNode(arrayOf(Label{"Movie"}, Label{"Entity"}), mapOf("text" to "The Notebook", "type" to "Movie"))
            val titanicNode = VirtualNode(arrayOf(Label{"Movie"}, Label{"Entity"}), mapOf("text" to "Titanic", "type" to "Movie"))
            val topBoyNode = VirtualNode(arrayOf(Label{"Television"}, Label{"Entity"}), mapOf("text" to "Top Boy", "type" to "Television"))

            assertThat(nodes, hasItem(NodeMatcher(matrixNode.labels.toList(), matrixNode.allProperties)))
            assertThat(nodes, hasItem(NodeMatcher(notebookNode.labels.toList(), notebookNode.allProperties)))
            assertThat(nodes, hasItem(NodeMatcher(titanicNode.labels.toList(), titanicNode.allProperties)))
            assertThat(nodes, hasItem(NodeMatcher(topBoyNode.labels.toList(), topBoyNode.allProperties)))

            val relationships = virtualGraph.graph["relationships"] as Set<*>

            assertEquals(5, relationships.size)
            assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, matrixNode, "ENTITY", mapOf("score" to 0.9F))))
            assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, notebookNode, "ENTITY", mapOf("score" to 0.9F))))

            assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, matrixNode, "ENTITY", mapOf("score" to 0.9F))))
            assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, titanicNode, "ENTITY", mapOf("score" to 0.9F))))
            assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, topBoyNode, "ENTITY", mapOf("score" to 0.9F))))

            // verify real graph
            assertEquals(6L, it.allNodes.stream().count())

            val dbMatrixNode = it.findNode({ "Movie" }, "text", "The Matrix")
            assertNotNull(dbMatrixNode)
            val dbNotebookNode = it.findNode({ "Movie" }, "text", "The Notebook")
            assertNotNull(dbNotebookNode)
            val dbTitanicNode = it.findNode({ "Movie" }, "text", "Titanic")
            assertNotNull(dbTitanicNode)
            val dbTopBoyNode = it.findNode({ "Television" }, "text", "Top Boy")
            assertNotNull(dbTopBoyNode)

            val allRelationships = it.allRelationships.stream().collect(Collectors.toList())

            assertEquals(5, allRelationships.size)

            assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, dbMatrixNode, "ENTITY", mapOf("score" to 0.9F))))
            assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, dbNotebookNode, "ENTITY", mapOf("score" to 0.9F))))

            assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, dbMatrixNode, "ENTITY", mapOf("score" to 0.9F))))
            assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, dbTitanicNode, "ENTITY", mapOf("score" to 0.9F))))
            assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, dbTopBoyNode, "ENTITY", mapOf("score" to 0.9F))))
        }


    }

}

