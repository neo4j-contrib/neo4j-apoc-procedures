package apoc.nlp.gcp

import apoc.nlp.NodeMatcher
import apoc.nlp.RelationshipMatcher
import apoc.result.NodeValueErrorMapResult
import apoc.result.VirtualNode
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.hasItem
import org.junit.ClassRule
import org.junit.Test
import org.neo4j.configuration.SettingImpl
import org.neo4j.configuration.SettingValueParsers
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.test.rule.ImpermanentDbmsRule
import java.util.stream.Collectors

class GCPVirtualEntitiesGraphStoreTest {
    companion object {
        @ClassRule
        @JvmField
        val neo4j = ImpermanentDbmsRule()
                .withSetting(SettingImpl.newBuilder("unsupported.dbms.debug.track_cursor_close", SettingValueParsers.BOOL, false).build(), false)
                .withSetting(SettingImpl.newBuilder("unsupported.dbms.debug.trace_cursors", SettingValueParsers.BOOL, false).build(), false)
    }

    @Test
    fun `store graph from result with multiple source nodes with overlapping entities`() {
        neo4j.beginTx().use {
            val res = listOf(
                    NodeValueErrorMapResult(null, mapOf("entities" to listOf(
                            mapOf("name" to "The Matrix", "type" to "OTHER", "salience" to 0.2),
                            mapOf("name" to "The Notebook", "type" to "PHONE_NUMBER", "salience" to 0.3)
                    )), mapOf()),
                    NodeValueErrorMapResult(null, mapOf("entities" to listOf(
                            mapOf("name" to "The Matrix", "type" to "OTHER", "salience" to 0.35),
                            mapOf("name" to "Toy Story", "type" to "OTHER", "salience" to 0.4),
                            mapOf("name" to "Titanic", "type" to "WORK_OF_ART", "salience" to 0.5)
                    )), mapOf())
            )


            val sourceNode1 = it.createNode(Label {"Person"})
            sourceNode1.setProperty("id", 1234L)
            val sourceNode2 = it.createNode(Label {"Person"})
            sourceNode2.setProperty("id", 5678L)

            val virtualGraph = GCPVirtualEntitiesGraph(res, listOf(sourceNode1, sourceNode2), RelationshipType { "ENTITY" }, "score", 0.0).createAndStore(it)

            // verify virtual graph
            val nodes = virtualGraph.graph["nodes"] as Set<*>
            assertEquals(6, nodes.size)
            assertThat(nodes, hasItem(sourceNode1))
            assertThat(nodes, hasItem(sourceNode2))

            val matrixNode = VirtualNode(arrayOf(Label{"Other"}, Label{"Entity"}), mapOf("text" to "The Matrix", "type" to "OTHER"))
            val notebookNode = VirtualNode(arrayOf(Label{"PhoneNumber"}, Label{"Entity"}), mapOf("text" to "The Notebook", "type" to "PHONE_NUMBER"))
            val toyStoryNode = VirtualNode(arrayOf(Label{"Other"}, Label{"Entity"}), mapOf("text" to "Toy Story", "type" to "OTHER"))
            val titanicNode = VirtualNode(arrayOf(Label{"WorkOfArt"}, Label{"Entity"}), mapOf("text" to "Titanic", "type" to "WORK_OF_ART"))

            assertThat(nodes, hasItem(NodeMatcher(matrixNode.labels.toList(), matrixNode.allProperties)))
            assertThat(nodes, hasItem(NodeMatcher(notebookNode.labels.toList(), notebookNode.allProperties)))
            assertThat(nodes, hasItem(NodeMatcher(toyStoryNode.labels.toList(), toyStoryNode.allProperties)))
            assertThat(nodes, hasItem(NodeMatcher(titanicNode.labels.toList(), titanicNode.allProperties)))

            val relationships = virtualGraph.graph["relationships"] as Set<*>

            assertEquals(5, relationships.size)
            assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, matrixNode, "ENTITY", mapOf("score" to 0.2))))
            assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, notebookNode, "ENTITY", mapOf("score" to 0.3))))

            assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, matrixNode, "ENTITY", mapOf("score" to 0.35))))
            assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, toyStoryNode, "ENTITY", mapOf("score" to 0.4))))
            assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, titanicNode, "ENTITY", mapOf("score" to 0.5))))

            // verify real graph
            assertEquals(6L, it.allNodes.stream().count())

            val dbMatrixNode = it.findNode({ "Other" }, "text", "The Matrix")
            assertNotNull(dbMatrixNode)
            val dbNotebookNode = it.findNode({ "PhoneNumber" }, "text", "The Notebook")
            assertNotNull(dbNotebookNode)
            val dbTitanicNode = it.findNode({ "WorkOfArt" }, "text", "Titanic")
            assertNotNull(dbTitanicNode)
            val dbToyStoryNode = it.findNode({ "Other" }, "text", "Toy Story")
            assertNotNull(dbToyStoryNode)

            val allRelationships = it.allRelationships.stream().collect(Collectors.toList())

            assertEquals(5, allRelationships.size)

            assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, dbMatrixNode, "ENTITY", mapOf("score" to 0.2))))
            assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, dbNotebookNode, "ENTITY", mapOf("score" to 0.3))))

            assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, dbMatrixNode, "ENTITY", mapOf("score" to 0.35))))
            assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, dbToyStoryNode, "ENTITY", mapOf("score" to 0.4))))
            assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, dbTitanicNode, "ENTITY", mapOf("score" to 0.5))))
        }


    }

}

