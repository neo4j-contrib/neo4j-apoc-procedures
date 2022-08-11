package apoc.nlp

import apoc.nlp.aws.AWSProceduresAPITest
import org.hamcrest.Description
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship

data class RelationshipMatcher(private val startNode: Node?, private val endNode: Node?, private val relationshipType: String, private val properties: Map<String, Any> = mapOf()) : org.hamcrest.TypeSafeDiagnosingMatcher<Relationship>() {
    override fun describeTo(description: Description?) {
        description?.appendText("startNode: ")
                ?.appendValue(startNode)?.appendText(", endNode: ")
                ?.appendValue(endNode)?.appendText(", relType: ")
                ?.appendValue(relationshipType)?.appendText(", relProperties: ")?.appendValue(properties)
    }

    override fun matchesSafely(item: Relationship?, mismatchDescription: Description?): Boolean {
        val startNodeMatches = nodeMatches(item?.startNode, startNode)
        val endNodeMatches = nodeMatches(item?.endNode, endNode)
        val relationshipMatches = item?.type?.name() == relationshipType
        val relPropertiesMatch = AWSProceduresAPITest.propertiesMatch(properties, item?.allProperties)

        if (startNodeMatches && endNodeMatches && relationshipMatches && relPropertiesMatch) {
            return true
        }

        mismatchDescription!!
                .appendText("got ")
                .appendText("{ startNode: ")
                .appendValue(item?.startNode?.labels)
                .appendText(", endNode: ").appendValue(item?.endNode?.labels)
                .appendText(", relationshipType: ").appendValue(item?.type?.name())
                .appendText(", relationshipProperties: ").appendValue(item?.allProperties)
                .appendText(" }")
        return false

    }

    private fun nodeMatches(one: Node?, two: Node?): Boolean {
        return AWSProceduresAPITest.nodeMatches(one, two?.labels?.map { l -> l.name() }, two?.allProperties?.toMap())
    }
}