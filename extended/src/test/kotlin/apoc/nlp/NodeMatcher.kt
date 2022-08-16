package apoc.nlp

import org.hamcrest.Description
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import java.util.stream.Collectors

data class NodeMatcher(private val labels: List<Label>, private val properties: Map<String, Any>) : org.hamcrest.TypeSafeDiagnosingMatcher<Node>() {
    companion object  {
        fun propertiesMatch(expected: Map<String, Any>?, actual: Map<String, Any>?) =
                actual?.keys == expected?.keys && actual!!.all { entry -> expected?.containsKey(entry.key)!! && expected[entry.key] == entry.value }

        fun nodeMatches(item: Node?, labels: List<String>?, properties: Map<String, Any>?): Boolean {
            val labelsMatched = item?.labels!!.count() == labels?.size  && item.labels!!.all { label -> labels?.contains(label.name()) }
            val propertiesMatches = propertiesMatch(properties, item.allProperties)
            return labelsMatched && propertiesMatches
        }
    }

    constructor(node: Node) : this(node.labels.toList(), node.allProperties)

    private val labelNames: List<String> = labels.stream().map { l -> l.name()}.collect(Collectors.toList())

    override fun describeTo(description: Description?) {
        description?.appendText("a node with labels ")?.appendValue(labelNames)?.appendText(" a node with properties ")?.appendValue(properties)
    }

    override fun matchesSafely(item: Node?, mismatchDescription: Description?): Boolean {
        val nodeMatches = NodeMatcher.nodeMatches(item, labelNames, properties)
        if(!nodeMatches) {
            mismatchDescription!!
                    .appendText("got ").appendText("labels: ").appendValue(item?.labels?.map { l -> l.name() })
                    .appendText(",  properties:").appendValue(item?.allProperties)
            return false
        }
        return true
    }
}