package apoc.nlp.aws

import org.hamcrest.Description
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship

data class RelationshipMatcher(private val startNode: Node?, private val endNode: Node?, private val relationshipType: String) : org.hamcrest.TypeSafeDiagnosingMatcher<Relationship>() {
    override fun describeTo(description: Description?) {
        description?.appendText("startNode: ")
                ?.appendValue(startNode)?.appendText(", endNode: ")
                ?.appendValue(endNode)?.appendText(", relType: ")
                ?.appendValue(relationshipType)
    }

    override fun matchesSafely(item: Relationship?, mismatchDescription: Description?): Boolean {
        val startNodeMatches = nodeMatches(item?.startNode, startNode)
        val endNodeMatches = nodeMatches(item?.endNode, endNode)
        val relationshipMatches = item?.type?.name() == relationshipType

        if (startNodeMatches && endNodeMatches && relationshipMatches) {
            return true
        }

        mismatchDescription!!
                .appendText("got ").appendText("{startNode: ").appendValue(item?.startNode?.labels)
                .appendText(", endNode: ").appendValue(item?.endNode?.labels)
                .appendText(", relationshipType: ").appendValue(item?.type?.name()).appendText("}")
        return false

    }

    private fun nodeMatches(one: Node?, two: Node?): Boolean {
        return AWSProceduresAPITest.nodeMatches(one, two?.labels?.map { l -> l.name() }, two?.allProperties?.toMap())
    }
}