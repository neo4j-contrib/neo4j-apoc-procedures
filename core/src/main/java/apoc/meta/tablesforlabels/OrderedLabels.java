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
package apoc.meta.tablesforlabels;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.neo4j.graphdb.Label;

/**
 * Abstraction on an ordered label set, used as a key for tables for labels profiles
 */
public class OrderedLabels {
    List<String> labels;

    public OrderedLabels(Iterable<Label> input) {
        labels = new ArrayList<>(3);
        for (Label l : input) {
            labels.add(l.name());
        }

        Collections.sort(labels);
    }

    @Override
    public int hashCode() {
        return labels.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof OrderedLabels && nodeLabels().equals(((OrderedLabels) o).nodeLabels());
    }

    public String asNodeType() {
        return ":" + labels.stream().map(s -> "`" + s + "`").collect(Collectors.joining(":"));
    }

    public List<String> nodeLabels() {
        return labels;
    }
}
