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

import java.util.Map;
import java.util.stream.Collectors;
import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;

public class EntityUtil {

    public static <T> T anyRebind(Transaction tx, T any) {
        if (any instanceof Map) {
            return (T) ((Map<String, Object>) any)
                    .entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> anyRebind(tx, e.getValue())));
        }
        if (any instanceof Path) {
            final Path path = (Path) any;
            PathImpl.Builder builder = new PathImpl.Builder(Util.rebind(tx, path.startNode()));
            for (Relationship rel : path.relationships()) {
                builder = builder.push(Util.rebind(tx, rel));
            }
            return (T) builder.build();
        }
        if (any instanceof Iterable) {
            return (T)
                    Iterables.stream((Iterable) any).map(i -> anyRebind(tx, i)).collect(Collectors.toList());
        }
        if (any instanceof Entity) {
            return (T) Util.rebind(tx, (Entity) any);
        }
        return any;
    }
}
