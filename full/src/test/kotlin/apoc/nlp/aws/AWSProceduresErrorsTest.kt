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
package apoc.nlp.aws

import apoc.util.TestUtil
import org.hamcrest.CoreMatchers.containsString
import org.junit.Assert.assertThat
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Test
import org.junit.jupiter.api.Assertions
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.test.rule.ImpermanentDbmsRule

class AWSProceduresErrorsTest {
    companion object {
        @ClassRule
        @JvmField
        val neo4j = ImpermanentDbmsRule()

        @BeforeClass
        @JvmStatic
        fun beforeClass() {
            TestUtil.registerProcedure(neo4j, AWSProcedures::class.java)
        }
    }

    @Test
    fun `should throw exception when given invalid source`() {
        val exception = Assertions.assertThrows(QueryExecutionException::class.java) {
            neo4j.executeTransactionally("""
                    CALL apoc.nlp.aws.entities.stream(["blah"])
                    YIELD node, value, error
                    RETURN node, value, error
                """.trimIndent(), mapOf()) {
                println(it.resultAsString())
            }
        }
        assertThat(exception.message, containsString("java.lang.IllegalArgumentException: `source` must be a node or list of nodes, but was: `[blah]`"))
    }

    @Test
    fun `should throw exception when given node with no properties`() {
        neo4j.executeTransactionally("""CREATE (a:Article1)""", mapOf())

        val exception = Assertions.assertThrows(QueryExecutionException::class.java) {
            neo4j.executeTransactionally("""
                    MATCH (a:Article1)
                    CALL apoc.nlp.aws.entities.stream(a)
                    YIELD node, value, error
                    RETURN node, value, error
                """.trimIndent(), mapOf()) {
                println(it.resultAsString())
            }
        }
        assertThat(exception.message, containsString("does not have property `text`. Property can be configured using parameter `nodeProperty`."))
    }


    @Test
    fun `should throw exception when key missing`() {
        neo4j.executeTransactionally("""CREATE (a:Article2 {text: 'foo'})""", mapOf())

        val exception = Assertions.assertThrows(QueryExecutionException::class.java) {
            neo4j.executeTransactionally("""
                    MATCH (a:Article2)
                    CALL apoc.nlp.aws.entities.stream(a)
                    YIELD node, value, error
                    RETURN node, value, error
                """.trimIndent(), mapOf()) {
                println(it.resultAsString())
            }
        }
        assertThat(exception.message, containsString("java.lang.IllegalArgumentException: Missing parameter `key`"))
    }

    @Test
    fun `should throw exception when secret missing`() {
        neo4j.executeTransactionally("""CREATE (a:Article3 {text: 'foo'})""", mapOf())

        val exception = Assertions.assertThrows(QueryExecutionException::class.java) {
            neo4j.executeTransactionally("""
                    MATCH (a:Article3)
                    CALL apoc.nlp.aws.entities.stream(a, {key: ${'$'}key})
                    YIELD node, value, error
                    RETURN node, value, error
                """.trimIndent(), mapOf("key" to "someKey")) {
                println(it.resultAsString())
            }
        }
        assertThat(exception.message, containsString("java.lang.IllegalArgumentException: Missing parameter `secret`"))
    }
}