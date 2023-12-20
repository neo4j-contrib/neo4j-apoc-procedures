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
package apoc.export.json;

import static apoc.export.util.FormatUtils.getLabelsSorted;

import apoc.export.util.ExportConfig;
import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public enum JsonFormatSerializer {
    DEFAULT() {

        @Override
        public void writeNode(JsonGenerator jsonGenerator, Node node, ExportConfig config) throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("type", "node");
            writeNodeDetails(jsonGenerator, node, config.writeNodeProperties());
            jsonGenerator.writeEndObject();
        }

        @Override
        public void writeRelationship(JsonGenerator jsonGenerator, Relationship rel, ExportConfig config)
                throws IOException {
            Node startNode = rel.getStartNode();
            Node endNode = rel.getEndNode();
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("type", "relationship");
            writeRelationshipDetails(jsonGenerator, rel, config.writeNodeProperties());
            writeRelationshipNode(jsonGenerator, "start", startNode, config);
            writeRelationshipNode(jsonGenerator, "end", endNode, config);
            jsonGenerator.writeEndObject();
        }

        @Override
        public void serializeProperties(JsonGenerator jsonGenerator, Map<String, Object> properties)
                throws IOException {
            if (properties != null && !properties.isEmpty()) {
                jsonGenerator.writeObjectFieldStart("properties");
                for (Map.Entry<String, Object> entry : properties.entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    serializeProperty(jsonGenerator, key, value, true);
                }
                jsonGenerator.writeEndObject();
            }
        }

        @Override
        public void serializeProperty(JsonGenerator jsonGenerator, String key, Object value, boolean writeKey)
                throws IOException {
            if (value == null) {
                if (writeKey) {
                    jsonGenerator.writeNullField(key);
                } else {
                    jsonGenerator.writeNull();
                }
            } else {
                if (writeKey) {
                    jsonGenerator.writeObjectField(key, value);
                } else {
                    jsonGenerator.writeObject(value);
                }
            }
        }

        private void writeNodeDetails(JsonGenerator jsonGenerator, Node node, boolean writeProperties)
                throws IOException {
            jsonGenerator.writeStringField("id", String.valueOf(node.getId()));

            if (node.getLabels().iterator().hasNext()) {
                jsonGenerator.writeArrayFieldStart("labels");

                List<String> labels = getLabelsSorted(node);
                for (String label : labels) {
                    jsonGenerator.writeString(label);
                }
                jsonGenerator.writeEndArray();
            }
            if (writeProperties) {
                serializeProperties(jsonGenerator, node.getAllProperties());
            }
        }

        private void writeRelationshipDetails(JsonGenerator jsonGenerator, Relationship rel, boolean writeProperties)
                throws IOException {
            jsonGenerator.writeStringField("id", String.valueOf(rel.getId()));
            jsonGenerator.writeStringField("label", rel.getType().toString());

            if (writeProperties) {
                serializeProperties(jsonGenerator, rel.getAllProperties());
            }
        }

        private void writeRelationshipNode(JsonGenerator jsonGenerator, String type, Node node, ExportConfig config)
                throws IOException {
            jsonGenerator.writeObjectFieldStart(type);

            writeNodeDetails(jsonGenerator, node, config.writeNodeProperties());
            jsonGenerator.writeEndObject();
        }
    };

    public abstract void writeNode(JsonGenerator jsonGenerator, Node node, ExportConfig config) throws IOException;

    public abstract void writeRelationship(JsonGenerator jsonGenerator, Relationship relationship, ExportConfig config)
            throws IOException;

    public abstract void serializeProperties(JsonGenerator jsonGenerator, Map<String, Object> properties)
            throws IOException;

    public abstract void serializeProperty(JsonGenerator jsonGenerator, String key, Object value, boolean writeKey)
            throws IOException;
}
