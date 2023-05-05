/**
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.nlp

import apoc.nlp.aws.AWSProceduresAPITest
import org.hamcrest.Description
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import java.util.stream.Collectors

data class NodeMatcher(private val labels: List<Label>, private val properties: Map<String, Any>) : org.hamcrest.TypeSafeDiagnosingMatcher<Node>() {
    constructor(node: Node) : this(node.labels.toList(), node.allProperties)
//    constructor(node: Node) : this(emptyList(), emptyMap())
    
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