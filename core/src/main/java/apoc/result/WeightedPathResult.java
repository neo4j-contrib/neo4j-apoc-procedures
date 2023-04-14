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

import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author mh
 * @since 12.05.16
 */
public class WeightedPathResult { // TODO: derive from PathResult when access to derived properties is fixed for yield
    public Path path;
    public double weight;

    public WeightedPathResult(WeightedPath weightedPath) {
        this.path = weightedPath;
        this.weight = weightedPath.weight();
    }

    public static Stream<WeightedPathResult> streamWeightedPathResult(Node startNode, Node endNode, PathFinder<WeightedPath> algo) {
        Iterable<WeightedPath> allPaths = algo.findAllPaths(startNode, endNode);
        return StreamSupport.stream(allPaths.spliterator(), false)
                .map(WeightedPathResult::new);
    }
}
