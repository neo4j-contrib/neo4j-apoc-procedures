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

import static org.hamcrest.MatcherAssert.assertThat;

import apoc.graph.util.GraphsConfig;
import apoc.util.MapUtil;
import java.util.HashMap;
import java.util.Map;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.neo4j.graphdb.Label;

public class LabelBuilderTest {

    @Test
    public void firstLetterOfWordCapitalised() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("mappings", MapUtil.map("$", "keyPhrase{!text,@metadata}"));

        LabelBuilder labelBuilder = new LabelBuilder(new GraphsConfig(conf));
        Label[] labels = labelBuilder.buildLabel(new HashMap<>(), "$");

        assertThat(labels, Matchers.arrayContaining(Label.label("Keyphrase")));
    }

    @Test
    public void firstLetterOfAllCapsWordCapitalised() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("mappings", MapUtil.map("$", "COMMERCIAL_ITEM{!text,@metadata}"));

        LabelBuilder labelBuilder = new LabelBuilder(new GraphsConfig(conf));
        Label[] labels = labelBuilder.buildLabel(new HashMap<>(), "$");

        assertThat(labels, Matchers.arrayContaining(Label.label("CommercialItem")));
    }

    @Test
    public void removeUnderscores() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("mappings", MapUtil.map("$", "Key_Phrase{!text,@metadata}"));

        LabelBuilder labelBuilder = new LabelBuilder(new GraphsConfig(conf));
        Label[] labels = labelBuilder.buildLabel(new HashMap<>(), "$");

        assertThat(labels, Matchers.arrayContaining(Label.label("KeyPhrase")));
    }

    @Test
    public void firstLetterOfWordsCapitalised() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("mappings", MapUtil.map("$", "KeyPhrase{!text,@metadata}"));

        LabelBuilder labelBuilder = new LabelBuilder(new GraphsConfig(conf));
        Label[] labels = labelBuilder.buildLabel(new HashMap<>(), "$");

        assertThat(labels, Matchers.arrayContaining(Label.label("Keyphrase")));
    }
}
