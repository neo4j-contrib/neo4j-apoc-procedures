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