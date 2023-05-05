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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * @author mh
 * @since 26.02.16
 */
public class WeightedRelationshipResult {
    public final Relationship rel;
    public final double weight;
    public final Node start;
    public final Node end;

    public WeightedRelationshipResult(Relationship rel, double weight) {
        this.rel = rel;
        this.weight = weight;
        this.start = rel.getStartNode();
        this.end = rel.getEndNode();
    }
}
