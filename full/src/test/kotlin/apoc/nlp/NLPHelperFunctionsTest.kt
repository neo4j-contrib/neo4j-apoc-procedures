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

import apoc.result.VirtualNode
import org.junit.Assert.assertEquals
import org.junit.Test

class NLPHelperFunctionsTest {
    @Test
    fun `should partition sources`() {
        assertEquals(
                listOf(listOf(VirtualNode(1), VirtualNode(2), VirtualNode(3)), listOf(VirtualNode(4))),
                NLPHelperFunctions.partition(listOf(VirtualNode(1), VirtualNode(2), VirtualNode(3), VirtualNode(4)), 3)
        )

        assertEquals(
                listOf(listOf(VirtualNode(1), VirtualNode(2), VirtualNode(3))),
                NLPHelperFunctions.partition(listOf(VirtualNode(1), VirtualNode(2), VirtualNode(3)), 3)
        )

        assertEquals(
                listOf(listOf(VirtualNode(1)), listOf(VirtualNode(2)), listOf(VirtualNode(3))),
                NLPHelperFunctions.partition(listOf(VirtualNode(1), VirtualNode(2), VirtualNode(3)), 1)
        )
    }

    @Test
    fun `relationship types`() {
        assertEquals("FOO", NLPHelperFunctions.entityRelationshipType(mapOf("writeRelationshipType" to "FOO", "relationshipType" to "BAR")).name())
        assertEquals("FOO", NLPHelperFunctions.entityRelationshipType(mapOf("relationshipType" to "FOO")).name())
        assertEquals("ENTITY", NLPHelperFunctions.entityRelationshipType(mapOf()).name())

        assertEquals("FOO", NLPHelperFunctions.categoryRelationshipType(mapOf("writeRelationshipType" to "FOO", "relationshipType" to "BAR")).name())
        assertEquals("FOO", NLPHelperFunctions.categoryRelationshipType(mapOf("relationshipType" to "FOO")).name())
        assertEquals("CATEGORY", NLPHelperFunctions.categoryRelationshipType(mapOf()).name())

        assertEquals("FOO", NLPHelperFunctions.keyPhraseRelationshipType(mapOf("writeRelationshipType" to "FOO", "relationshipType" to "BAR")).name())
        assertEquals("FOO", NLPHelperFunctions.keyPhraseRelationshipType(mapOf("relationshipType" to "FOO")).name())
        assertEquals("KEY_PHRASE", NLPHelperFunctions.keyPhraseRelationshipType(mapOf()).name())
    }
}
