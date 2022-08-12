package apoc.nlp.azure

import apoc.nlp.NodeMatcher
import apoc.nlp.RelationshipMatcher
import apoc.result.VirtualNode
import junit.framework.Assert.assertEquals
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.hasItem
import org.junit.Test
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType

class AzureVirtualEntitiesGraphTest {
    @Test
    fun `create virtual graph from result with one entity`() {
        val sourceNode = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))

        val res = listOf(
                mapOf("id" to sourceNode.id.toString(), "entities" to listOf(mapOf(
                        "name" to "foo",
                        "type" to "Person",
                        "matches" to listOf(
                                mapOf("entityTypeScore" to 0.9, "text" to "foo"),
                                mapOf("entityTypeScore" to 0.8, "text" to "foobar")
                        )
                )))
        )

        val virtualGraph = AzureVirtualEntitiesGraph(res, listOf(sourceNode), RelationshipType { "ENTITY" }, "score", 0.0).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(2, nodes.size)
        assertThat(nodes, hasItem(sourceNode))

        val barLabels = listOf(Label { "Person" }, Label { "Entity" })
        val barProperties = mapOf("text" to "foo", "type" to "Person")
        assertThat(nodes, hasItem(NodeMatcher(barLabels, barProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(1, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode, VirtualNode(barLabels.toTypedArray(), barProperties), "ENTITY", mapOf("score" to 0.9))))
    }

    @Test
    fun `create virtual graph from result with multiple entities`() {
        val sourceNode = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))

        val res = listOf(
                mapOf("id" to sourceNode.id.toString(), "entities" to listOf(
                        mapOf(
                                "name" to "The Matrix",
                                "type" to "Other",
                                "matches" to listOf(
                                        mapOf("entityTypeScore" to 0.6, "text" to "foo"),
                                        mapOf("entityTypeScore" to 0.5, "text" to "foobar")
                                )
                        ),
                        mapOf(
                                "name" to "The Notebook",
                                "type" to "Other",
                                "matches" to listOf(
                                        mapOf("entityTypeScore" to 0.7, "text" to "foo"),
                                        mapOf("entityTypeScore" to 0.5, "text" to "foobar")
                                )
                        )))
        )

        val virtualGraph = AzureVirtualEntitiesGraph(res, listOf(sourceNode), RelationshipType { "ENTITY" },"score", 0.0).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(3, nodes.size)
        assertThat(nodes, hasItem(sourceNode))

        val matrixNode = VirtualNode(arrayOf(Label{"Other"}, Label{"Entity"}), mapOf("text" to "The Matrix", "type" to "Other"))
        val notebookNode = VirtualNode(arrayOf(Label{"Other"}, Label{"Entity"}), mapOf("text" to "The Notebook", "type" to "Other"))

        assertThat(nodes, hasItem(NodeMatcher(matrixNode.labels.toList(), matrixNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(notebookNode.labels.toList(), notebookNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(2, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode, matrixNode, "ENTITY", mapOf("score" to 0.6))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode, notebookNode, "ENTITY", mapOf("score" to 0.7))))
    }

    @Test
    fun `create virtual graph from result with duplicate entities`() {
        val sourceNode = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))

        val res = listOf(
                mapOf("id" to sourceNode.id.toString(), "entities" to listOf(
                        mapOf(
                                "name" to "The Matrix",
                                "type" to "Other",
                                "matches" to listOf(
                                        mapOf("entityTypeScore" to 0.6, "text" to "foo"),
                                        mapOf("entityTypeScore" to 0.5, "text" to "foobar")
                                )
                        ),
                        mapOf(
                                "name" to "The Matrix",
                                "type" to "Other",
                                "matches" to listOf(
                                        mapOf("entityTypeScore" to 0.7, "text" to "foo"),
                                        mapOf("entityTypeScore" to 0.5, "text" to "foobar")
                                )
                        )))
        )

        val virtualGraph = AzureVirtualEntitiesGraph(res, listOf(sourceNode), RelationshipType { "ENTITY" },"score", 0.0).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(2, nodes.size)
        assertThat(nodes, hasItem(sourceNode))

        val matrixNode = VirtualNode(arrayOf(Label{"Other"}, Label{"Entity"}), mapOf("text" to "The Matrix", "type" to "Other"))

        assertThat(nodes, hasItem(NodeMatcher(matrixNode.labels.toList(), matrixNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(1, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode, matrixNode, "ENTITY", mapOf("score" to 0.7))))
    }

    @Test
    fun `create virtual graph from result with multiple source nodes`() {
        val sourceNode1 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))
        val sourceNode2 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 5678L))

        val res = listOf(
                mapOf("id" to sourceNode1.id.toString(), "entities" to listOf(
                        mapOf(
                                "name" to "The Matrix",
                                "type" to "Other",
                                "matches" to listOf(
                                        mapOf("entityTypeScore" to 0.2)
                                )
                        ),
                        mapOf(
                                "name" to "The Notebook",
                                "type" to "PhoneNumber",
                                "matches" to listOf(
                                        mapOf("entityTypeScore" to 0.3)
                                )
                        ))),
                mapOf("id" to sourceNode2.id.toString(), "entities" to listOf(
                        mapOf(
                                "name" to "Toy Story",
                                "type" to "Other",
                                "matches" to listOf(
                                        mapOf("entityTypeScore" to 0.4)
                                )
                        ),
                        mapOf(
                                "name" to "Titanic",
                                "type" to "Quantity",
                                "matches" to listOf(
                                        mapOf("entityTypeScore" to 0.5)
                                )
                        )))

        )

        val virtualGraph = AzureVirtualEntitiesGraph(res, listOf(sourceNode1, sourceNode2), RelationshipType { "ENTITY" },"score", 0.0).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(6, nodes.size)
        assertThat(nodes, hasItem(sourceNode1))
        assertThat(nodes, hasItem(sourceNode2))

        val matrixNode = VirtualNode(arrayOf(Label{"Other"}, Label{"Entity"}), mapOf("text" to "The Matrix", "type" to "Other"))
        val notebookNode = VirtualNode(arrayOf(Label{"PhoneNumber"}, Label{"Entity"}), mapOf("text" to "The Notebook", "type" to "PhoneNumber"))
        val toyStoryNode = VirtualNode(arrayOf(Label{"Other"}, Label{"Entity"}), mapOf("text" to "Toy Story", "type" to "Other"))
        val titanicNode = VirtualNode(arrayOf(Label{"Quantity"}, Label{"Entity"}), mapOf("text" to "Titanic", "type" to "Quantity"))

        assertThat(nodes, hasItem(NodeMatcher(matrixNode.labels.toList(), matrixNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(notebookNode.labels.toList(), notebookNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(toyStoryNode.labels.toList(), toyStoryNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(titanicNode.labels.toList(), titanicNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(4, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, matrixNode, "ENTITY", mapOf("score" to 0.2))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, notebookNode, "ENTITY", mapOf("score" to 0.3))))

        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, toyStoryNode, "ENTITY", mapOf("score" to 0.4))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, titanicNode, "ENTITY", mapOf("score" to 0.5))))
    }

    @Test
    fun `create virtual graph from result with multiple source nodes with overlapping entities`() {
        val sourceNode1 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))
        val sourceNode2 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 5678L))

        val res = listOf(
                mapOf("id" to sourceNode1.id.toString(), "entities" to listOf(
                        mapOf(
                                "name" to "The Matrix",
                                "type" to "Other",
                                "matches" to listOf(
                                        mapOf("entityTypeScore" to 0.7)
                                )
                        ),
                        mapOf(
                                "name" to "The Notebook",
                                "type" to "PhoneNumber",
                                "matches" to listOf(
                                        mapOf("entityTypeScore" to 0.8)
                                )
                        ))),
                mapOf("id" to sourceNode2.id.toString(), "entities" to listOf(
                        mapOf(
                                "name" to "Titanic",
                                "type" to "Skill",
                                "matches" to listOf(
                                        mapOf("entityTypeScore" to 0.75)
                                )
                        ),
                        mapOf(
                                "name" to "The Matrix",
                                "type" to "Other",
                                "matches" to listOf(
                                        mapOf("entityTypeScore" to 0.9)
                                )
                        ),
                        mapOf(
                                "name" to "Top Boy",
                                "type" to "Email",
                                "matches" to listOf(
                                        mapOf("entityTypeScore" to 0.4)
                                )
                        )
                ))

        )

        val virtualGraph = AzureVirtualEntitiesGraph(res, listOf(sourceNode1, sourceNode2), RelationshipType { "ENTITY" },"score", 0.0).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(6, nodes.size)
        assertThat(nodes, hasItem(sourceNode1))
        assertThat(nodes, hasItem(sourceNode2))

        val matrixNode = VirtualNode(arrayOf(Label{"Other"}, Label{"Entity"}), mapOf("text" to "The Matrix", "type" to "Other"))
        val notebookNode = VirtualNode(arrayOf(Label{"PhoneNumber"}, Label{"Entity"}), mapOf("text" to "The Notebook", "type" to "PhoneNumber"))
        val titanicNode = VirtualNode(arrayOf(Label{"Skill"}, Label{"Entity"}), mapOf("text" to "Titanic", "type" to "Skill"))
        val topBoyNode = VirtualNode(arrayOf(Label{"Email"}, Label{"Entity"}), mapOf("text" to "Top Boy", "type" to "Email"))

        assertThat(nodes, hasItem(NodeMatcher(matrixNode.labels.toList(), matrixNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(notebookNode.labels.toList(), notebookNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(titanicNode.labels.toList(), titanicNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(topBoyNode.labels.toList(), topBoyNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(5, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, matrixNode, "ENTITY", mapOf("score" to 0.7))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, notebookNode, "ENTITY", mapOf("score" to 0.8))))

        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, titanicNode, "ENTITY", mapOf("score" to 0.75))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, matrixNode, "ENTITY", mapOf("score" to 0.9))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, topBoyNode, "ENTITY", mapOf("score" to 0.4))))
    }

    @Test
    fun `create graph based on confidence cut off`() {
        val sourceNode1 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))
        val sourceNode2 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 5678L))

        val res = listOf(
                mapOf("id" to sourceNode1.id.toString(), "entities" to listOf(
                        mapOf(
                                "name" to "The Matrix",
                                "type" to "Other",
                                "matches" to listOf(
                                        mapOf("entityTypeScore" to 0.7)
                                )
                        ),
                        mapOf(
                                "name" to "The Notebook",
                                "type" to "PhoneNumber",
                                "matches" to listOf(
                                        mapOf("wikipediaScore" to 0.8)
                                )
                        ))),
                mapOf("id" to sourceNode2.id.toString(), "entities" to listOf(
                        mapOf(
                                "name" to "Titanic",
                                "type" to "Skill",
                                "matches" to listOf(
                                        mapOf("wikipediaScore" to 0.75)
                                )
                        ),
                        mapOf(
                                "name" to "The Matrix",
                                "type" to "Other",
                                "matches" to listOf(
                                        mapOf("entityTypeScore" to 0.9)
                                )
                        ),
                        mapOf(
                                "name" to "Top Boy",
                                "type" to "Email",
                                "matches" to listOf(
                                        mapOf("wikipediaScore" to 0.4)
                                )
                        )
                ))

        )

        val virtualGraph = AzureVirtualEntitiesGraph(res, listOf(sourceNode1, sourceNode2), RelationshipType { "ENTITY" },"score", 0.75).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(5, nodes.size)
        assertThat(nodes, hasItem(sourceNode1))
        assertThat(nodes, hasItem(sourceNode2))

        val matrixNode = VirtualNode(arrayOf(Label{"Other"}, Label{"Entity"}), mapOf("text" to "The Matrix", "type" to "Other"))
        val notebookNode = VirtualNode(arrayOf(Label{"PhoneNumber"}, Label{"Entity"}), mapOf("text" to "The Notebook", "type" to "PhoneNumber"))
        val titanicNode = VirtualNode(arrayOf(Label{"Skill"}, Label{"Entity"}), mapOf("text" to "Titanic", "type" to "Skill"))

        assertThat(nodes, hasItem(NodeMatcher(matrixNode.labels.toList(), matrixNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(notebookNode.labels.toList(), notebookNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(titanicNode.labels.toList(), titanicNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(3, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, notebookNode, "ENTITY", mapOf("score" to 0.8))))

        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, titanicNode, "ENTITY", mapOf("score" to 0.75))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, matrixNode, "ENTITY", mapOf("score" to 0.9))))
    }

}

