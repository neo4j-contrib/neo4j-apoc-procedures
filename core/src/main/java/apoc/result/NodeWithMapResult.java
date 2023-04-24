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

import java.util.Collections;
import java.util.Map;

public class NodeWithMapResult {
    public final Node node;
    public final Map<String, Object> value;
    public final Map<String, Object> error;

    public NodeWithMapResult(Node node, Map<String, Object> value, Map<String, Object>  error) {
        this.node = node;
        this.value = value;
        this.error = error;
    }

    public static NodeWithMapResult withError(Node node, Map<String, Object> error) {
        return new NodeWithMapResult(node, Collections.emptyMap(), error);
    }

    public static NodeWithMapResult withResult(Node node, Map<String, Object> value) {
        return new NodeWithMapResult(node, value, Collections.emptyMap());
    }

}
