/*
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
package apoc.util;

import org.assertj.core.api.AbstractAssert;

import java.util.*;

public class MapAssertions extends AbstractAssert<MapAssertions, Map<String,Object>> {
    private final Collection<String> blackListedKeys;
    private final List<String> propertyPath;

    private MapAssertions(Map<String, Object> stringObjectMap, Collection<String> blackListedKeys, List<String> propertyPath) {
        super(stringObjectMap, MapAssertions.class);
        this.blackListedKeys = blackListedKeys;
        this.propertyPath = propertyPath;
    }

    public MapAssertions(Map<String, Object> stringObjectMap, Collection<String> blackListedKeys) {
        this(stringObjectMap, blackListedKeys, new ArrayList<>());
    }

    public static MapAssertions assertThat(Map<String, Object> actual, List<String> blackListedKeys) {
            return new MapAssertions(actual, blackListedKeys);
    }

    public MapAssertions isEqualsTo(Map<String, Object> expected) {
        isNotNull();

        if (actual.size() != expected.size() ) {
            failWithMessage("not same number of elements: %s actual has %d elements, expected %s has %d elements", actual, actual.size(), expected, expected.size());
        } else {

            actual.keySet().stream().filter(k -> !blackListedKeys.contains(k)).forEach(k -> {

                Object actualValue = actual.get(k);
                Object expectedValue = expected.get(k);

                if (actualValue instanceof Collection) {
                    System.out.println("coll");
                } else if (actualValue instanceof Map) {
                    System.out.println("map");
                } else {

                    if (!Objects.equals(actualValue, expectedValue)) {
                        failWithMessage("property %s differs: actual %s, expected %s", String.join(".", propertyPath) + "." + k, actualValue, expectedValue);
                    }

                }
            });
        }
        return this;
    }
}
