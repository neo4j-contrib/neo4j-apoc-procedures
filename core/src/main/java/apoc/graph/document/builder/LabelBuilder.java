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
package apoc.graph.document.builder;

import apoc.graph.util.GraphsConfig;
import apoc.text.Strings;
import org.neo4j.graphdb.Label;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.commons.text.WordUtils.capitalizeFully;

public class LabelBuilder {

    private GraphsConfig config;

    public LabelBuilder(GraphsConfig config) {
        this.config = config;
    }

    public Label[] buildLabel(Map<String, Object> obj, String path) {
        Strings strings = new Strings();

        List<String> rawLabels = new ArrayList<>();

        if (obj.containsKey(config.getLabelField())) {
            rawLabels.add(obj.get(config.getLabelField()).toString());
        }
        rawLabels.addAll(config.labelsForPath(path));
        return rawLabels.stream().map(label -> Label.label(capitalizeFully(label, '_', ' ')
                .replaceAll("_", "")
                .replaceAll(" ", "")))
                .toArray(Label[]::new);
    }

}
