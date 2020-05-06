package apoc.nlp

import apoc.nlp.aws.AWSProceduresAPITest
import org.hamcrest.Description
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import java.util.stream.Collectors

data class NodeMatcher(private val labels: List<Label>, private val properties: Map<String, Any>) : org.hamcrest.TypeSafeDiagnosingMatcher<Node>() {
    private val labelNames: List<String> = labels.stream().map { l -> l.name()}.collect(Collectors.toList())

    override fun describeTo(description: Description?) {
        description?.appendText("a node with labels ")?.appendValue(labelNames)?.appendText(" a node with properties ")?.appendValue(properties)
    }

    override fun matchesSafely(item: Node?, mismatchDescription: Description?): Boolean {
        val nodeMatches = AWSProceduresAPITest.nodeMatches(item, labelNames, properties)
        if(!nodeMatches) {
            mismatchDescription!!
                    .appendText("got ").appendText("labels: ").appendValue(item?.labels?.map { l -> l.name() })
                    .appendText(",  properties:").appendValue(item?.allProperties)
            return false
        }
        return true
    }
}