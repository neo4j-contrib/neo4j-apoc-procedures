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
package apoc.result;

import org.neo4j.graphdb.Path;

public class DistancePathResult implements Comparable<DistancePathResult> {

    public final Path path;

    public final double distance;

    public DistancePathResult(Path path, double distance) {
        this.path = path;
        this.distance = distance;
    }

    @Override
    public int compareTo(DistancePathResult o) {
        return o.distance < this.distance ? 1 : -1;
    }
}
