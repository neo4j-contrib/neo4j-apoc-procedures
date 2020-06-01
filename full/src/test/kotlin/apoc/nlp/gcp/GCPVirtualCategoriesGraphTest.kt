package apoc.nlp.gcp

import apoc.nlp.NodeMatcher
import apoc.nlp.RelationshipMatcher
import apoc.result.NodeValueErrorMapResult
import apoc.result.VirtualNode
import junit.framework.Assert.assertEquals
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.hasItem
import org.junit.Test
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType

class GCPVirtualCategoriesGraphTest {
    companion object {
        val RELATIONSHIP_TYPE = RelationshipType { "CATEGORY" }
        val RELATIONSHIP_PROPERTY = "score"
        val LABEL = Label{"Category"}
    }

    @Test
    fun `create virtual graph from result with one category`() {
        val sourceNode = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))

        val res = listOf(
                NodeValueErrorMapResult(null, mapOf("categories" to listOf(
                        mapOf("name" to "/Health/Public Health", "confidence" to 0.75))
                ), mapOf())
        )

        val virtualGraph = GCPVirtualClassificationGraph(res, listOf(sourceNode), RELATIONSHIP_TYPE, RELATIONSHIP_PROPERTY, 0.0).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(2, nodes.size)
        assertThat(nodes, hasItem(sourceNode))

        val labels = listOf(Label { "Category" })
        val properties = mapOf("text" to "/Health/Public Health")
        assertThat(nodes, hasItem(NodeMatcher(labels, properties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(1, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode, VirtualNode(labels.toTypedArray(), properties), RELATIONSHIP_TYPE.name(), mapOf("score" to 0.75))))
    }

    @Test
    fun `create virtual graph from result with multiple categories`() {
        val sourceNode = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))

        val res = listOf(
                NodeValueErrorMapResult(null, mapOf("categories" to listOf(
                        mapOf("name" to "/Health/Public Health", "confidence" to 0.75),
                        mapOf("name" to "/Health/Medical Facilities & Services", "confidence" to 0.85))
                ), mapOf())
        )

        val virtualGraph = GCPVirtualClassificationGraph(res, listOf(sourceNode), RELATIONSHIP_TYPE,RELATIONSHIP_PROPERTY, 0.0).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(3, nodes.size)
        assertThat(nodes, hasItem(sourceNode))

        val publicHealthNode = VirtualNode(arrayOf(LABEL), mapOf("text" to "/Health/Public Health"))
        val medicalNode = VirtualNode(arrayOf(LABEL), mapOf("text" to "/Health/Medical Facilities & Services"))

        assertThat(nodes, hasItem(NodeMatcher(publicHealthNode.labels.toList(), publicHealthNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(medicalNode.labels.toList(), medicalNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(2, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode, publicHealthNode, RELATIONSHIP_TYPE.name(), mapOf("score" to 0.75))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode, medicalNode, RELATIONSHIP_TYPE.name(), mapOf("score" to 0.85))))
    }

    @Test
    fun `create virtual graph from result with duplicate entities`() {
        val sourceNode = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))

        val res = listOf(
                NodeValueErrorMapResult(null, mapOf("categories" to listOf(
                        mapOf("name" to "/Health/Public Health", "confidence" to 0.75),
                        mapOf("name" to "/Health/Public Health", "confidence" to 0.85))
                ), mapOf())
        )

        val virtualGraph = GCPVirtualClassificationGraph(res, listOf(sourceNode), RELATIONSHIP_TYPE,RELATIONSHIP_PROPERTY, 0.0).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(2, nodes.size)
        assertThat(nodes, hasItem(sourceNode))

        val publicHealthNode = VirtualNode(arrayOf(LABEL), mapOf("text" to "/Health/Public Health"))

        assertThat(nodes, hasItem(NodeMatcher(publicHealthNode.labels.toList(), publicHealthNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(1, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode, publicHealthNode, RELATIONSHIP_TYPE.name(), mapOf("score" to 0.85))))
    }

    @Test
    fun `create virtual graph from result with multiple source nodes`() {
        val sourceNode1 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))
        val sourceNode2 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 5678L))

        val res = listOf(
                NodeValueErrorMapResult(null, mapOf("categories" to listOf(
                        mapOf("name" to "/Hobbies & Leisure/Outdoors", "confidence" to 0.2),
                        mapOf("name" to "/Hobbies & Leisure/Paintball", "confidence" to 0.3)
                )), mapOf()),
                NodeValueErrorMapResult(null, mapOf("categories" to listOf(
                        mapOf("name" to "/Arts & Entertainment/Music & Audio", "confidence" to 0.4),
                        mapOf("name" to "/Arts & Entertainment/Music & Audio/Classical Music", "confidence" to 0.5)
                )), mapOf())
        )

        val virtualGraph = GCPVirtualClassificationGraph(res, listOf(sourceNode1, sourceNode2), RELATIONSHIP_TYPE,RELATIONSHIP_PROPERTY, 0.0).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(6, nodes.size)
        assertThat(nodes, hasItem(sourceNode1))
        assertThat(nodes, hasItem(sourceNode2))

        val outdoorsNode = VirtualNode(arrayOf(LABEL), mapOf("text" to "/Hobbies & Leisure/Outdoors"))
        val paintballNode = VirtualNode(arrayOf(LABEL), mapOf("text" to "/Hobbies & Leisure/Paintball"))
        val musicNode = VirtualNode(arrayOf(LABEL), mapOf("text" to "/Arts & Entertainment/Music & Audio"))
        val classicalNode = VirtualNode(arrayOf(LABEL), mapOf("text" to "/Arts & Entertainment/Music & Audio/Classical Music"))

        assertThat(nodes, hasItem(NodeMatcher(outdoorsNode.labels.toList(), outdoorsNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(paintballNode.labels.toList(), paintballNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(musicNode.labels.toList(), musicNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(classicalNode.labels.toList(), classicalNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(4, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, outdoorsNode, RELATIONSHIP_TYPE.name(), mapOf("score" to 0.2))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, paintballNode, RELATIONSHIP_TYPE.name(), mapOf("score" to 0.3))))

        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, musicNode, RELATIONSHIP_TYPE.name(), mapOf("score" to 0.4))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, classicalNode, RELATIONSHIP_TYPE.name(), mapOf("score" to 0.5))))
    }

    @Test
    fun `create virtual graph from result with multiple source nodes with overlapping entities`() {
        val res = listOf(
                NodeValueErrorMapResult(null, mapOf("categories" to listOf(
                        mapOf("name" to "/Hobbies & Leisure/Outdoors", "confidence" to 0.7),
                        mapOf("name" to "/Hobbies & Leisure/Paintball", "confidence" to 0.8)
                )), mapOf()),
                NodeValueErrorMapResult(null, mapOf("categories" to listOf(
                        mapOf("name" to "/Arts & Entertainment/Music & Audio", "confidence" to 0.75),
                        mapOf("name" to "/Hobbies & Leisure/Outdoors", "confidence" to 0.9),
                        mapOf("name" to "/Arts & Entertainment/Music & Audio/Classical Music", "confidence" to 0.4)
                )), mapOf())
        )

        val sourceNode1 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))
        val sourceNode2 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 5678L))

        val virtualGraph = GCPVirtualClassificationGraph(res, listOf(sourceNode1, sourceNode2), RELATIONSHIP_TYPE,RELATIONSHIP_PROPERTY, 0.0).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(6, nodes.size)
        assertThat(nodes, hasItem(sourceNode1))
        assertThat(nodes, hasItem(sourceNode2))

        val outdoorsNode = VirtualNode(arrayOf(LABEL), mapOf("text" to "/Hobbies & Leisure/Outdoors"))
        val paintballNode = VirtualNode(arrayOf(LABEL), mapOf("text" to "/Hobbies & Leisure/Paintball"))
        val musicNode = VirtualNode(arrayOf(LABEL), mapOf("text" to "/Arts & Entertainment/Music & Audio"))
        val classicalNode = VirtualNode(arrayOf(LABEL), mapOf("text" to "/Arts & Entertainment/Music & Audio/Classical Music"))

        assertThat(nodes, hasItem(NodeMatcher(outdoorsNode.labels.toList(), outdoorsNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(paintballNode.labels.toList(), paintballNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(musicNode.labels.toList(), musicNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(classicalNode.labels.toList(), classicalNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(5, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, outdoorsNode, RELATIONSHIP_TYPE.name(), mapOf("score" to 0.7))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, paintballNode, RELATIONSHIP_TYPE.name(), mapOf("score" to 0.8))))

        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, musicNode, RELATIONSHIP_TYPE.name(), mapOf("score" to 0.75))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, outdoorsNode, RELATIONSHIP_TYPE.name(), mapOf("score" to 0.9))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, classicalNode, RELATIONSHIP_TYPE.name(), mapOf("score" to 0.4))))
    }

    @Test
    fun `create graph based on confidence cut off`() {
        val res = listOf(
                NodeValueErrorMapResult(null, mapOf("categories" to listOf(
                        mapOf("name" to "/Hobbies & Leisure/Outdoors", "confidence" to 0.7), // excluded
                        mapOf("name" to "/Hobbies & Leisure/Paintball", "confidence" to 0.8)
                )), mapOf()),
                NodeValueErrorMapResult(null, mapOf("categories" to listOf(
                        mapOf("name" to "/Arts & Entertainment/Music & Audio", "confidence" to 0.75),
                        mapOf("name" to "/Hobbies & Leisure/Outdoors", "confidence" to 0.9),
                        mapOf("name" to "/Arts & Entertainment/Music & Audio/Classical Music", "confidence" to 0.95),
                        mapOf("name" to "/Internet & Telecom/Web Services", "confidence" to 0.74) // excluded
                )), mapOf())
        )

        val sourceNode1 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))
        val sourceNode2 = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 5678L))

        val virtualGraph = GCPVirtualClassificationGraph(res, listOf(sourceNode1, sourceNode2), RELATIONSHIP_TYPE,RELATIONSHIP_PROPERTY, 0.75).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(6, nodes.size)
        assertThat(nodes, hasItem(sourceNode1))
        assertThat(nodes, hasItem(sourceNode2))

        val outdoorsNode = VirtualNode(arrayOf(LABEL), mapOf("text" to "/Hobbies & Leisure/Outdoors"))
        val paintballNode = VirtualNode(arrayOf(LABEL), mapOf("text" to "/Hobbies & Leisure/Paintball"))
        val musicNode = VirtualNode(arrayOf(LABEL), mapOf("text" to "/Arts & Entertainment/Music & Audio"))
        val classicalNode = VirtualNode(arrayOf(LABEL), mapOf("text" to "/Arts & Entertainment/Music & Audio/Classical Music"))

        assertThat(nodes, hasItem(NodeMatcher(outdoorsNode.labels.toList(), outdoorsNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(paintballNode.labels.toList(), paintballNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(musicNode.labels.toList(), musicNode.allProperties)))
        assertThat(nodes, hasItem(NodeMatcher(classicalNode.labels.toList(), classicalNode.allProperties)))

        val relationships = virtualGraph.graph["relationships"] as Set<*>

        assertEquals(4, relationships.size)
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode1, paintballNode, RELATIONSHIP_TYPE.name(), mapOf("score" to 0.8))))

        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, musicNode, RELATIONSHIP_TYPE.name(), mapOf("score" to 0.75))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, outdoorsNode, RELATIONSHIP_TYPE.name(), mapOf("score" to 0.9))))
        assertThat(relationships, hasItem(RelationshipMatcher(sourceNode2, classicalNode, RELATIONSHIP_TYPE.name(), mapOf("score" to 0.95))))
    }

}

