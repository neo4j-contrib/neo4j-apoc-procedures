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

import java.util.Collection;

public final class CollectionUtilsExtended {
    private CollectionUtilsExtended() {}

    /**
     * Null-safe check if the specified collection is empty.
     *
     * @param coll  the collection to check, may be null
     * @return true if empty or null
     */
    public static boolean isEmpty(Collection<?> coll) {
        return coll == null || coll.isEmpty();
    }

    /**
     * Null-safe check if the specified collection is not empty.
     *
     * @param coll  the collection to check, may be null
     * @return true if non-null and non-empty
     */
    public static boolean isNotEmpty(Collection<?> coll) {
        return !isEmpty(coll);
    }

    /**
     * Returns <code>true</code> iff at least one element is in both collections.
     *
     * @param <T> the type of object to lookup in <code>coll1</code>.
     * @param coll1  the first collection, must not be null
     * @param coll2  the second collection, must not be null
     * @return <code>true</code> iff the intersection of the collections is non-empty
     */
    public static <T> boolean containsAny(Collection<?> coll1, T... coll2) {
        if (coll1.size() < coll2.length) {
            for (Object aColl1 : coll1) {
                for (T t : coll2) {
                    if (t.equals(aColl1)) {
                        return true;
                    }
                }
            }
        } else {
            for (Object aColl2 : coll2) {
                if (coll1.contains(aColl2)) {
                    return true;
                }
            }
        }
        return false;
    }
}
