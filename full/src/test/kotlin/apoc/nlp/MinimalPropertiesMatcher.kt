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

import org.hamcrest.Description

data class MinimalPropertiesMatcher(private val properties: Map<String, Any>) : org.hamcrest.TypeSafeDiagnosingMatcher<Map<String, Any>>() {
    override fun describeTo(description: Description?) {
        description?.appendText(" a map containing ")?.appendValue(properties)
    }

    override fun matchesSafely(item: Map<String, Any>, mismatchDescription: Description?): Boolean {
        val propertiesMatch =  properties!!.all { entry -> item?.containsKey(entry.key)!! && item[entry.key] == entry.value }
        if(!propertiesMatch) {
            mismatchDescription!!.appendText(",  properties:").appendValue(item)
            return false
        }
        return true
    }

    companion object {
        fun hasAtLeast(properties: Map<String, Any>): MinimalPropertiesMatcher = MinimalPropertiesMatcher(properties)
    }
}