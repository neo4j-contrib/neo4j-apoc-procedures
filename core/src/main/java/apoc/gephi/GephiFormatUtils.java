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
package apoc.gephi;

import java.util.Iterator;
import java.util.Set;
import java.util.function.BiPredicate;
import org.neo4j.graphdb.Node;

public class GephiFormatUtils {

    public static String getCaption(Node n, Set<String> captions) {
        for (String caption : captions) { // first do one loop with the exact names
            if (n.hasProperty(caption)) return n.getProperty(caption).toString();
        }

        String result =
                filterCaption(n, captions, (key, caption) -> key.equalsIgnoreCase(caption)); // 2nd loop with lowercase
        if (result == null) {
            result = filterCaption(
                    n,
                    captions,
                    (key, caption) -> key.toLowerCase().contains(caption)
                            || key.toLowerCase().endsWith(caption)); // 3rd loop with contains or endsWith
            if (result == null) {
                Iterator<String> iterator = n.getPropertyKeys().iterator();
                if (iterator.hasNext()) {
                    result = n.getProperty(iterator.next()).toString(); // get the first property
                }
            }
        }
        return result == null ? String.valueOf(n.getId()) : result; // if the node has no property return the ID
    }

    public static String filterCaption(Node n, Set<String> captions, BiPredicate<String, String> predicate) {

        for (String caption : captions) {
            for (String key : n.getPropertyKeys()) {
                if (predicate.test(key, caption)) return n.getProperty(key).toString();
            }
        }
        return null;
    }
}
