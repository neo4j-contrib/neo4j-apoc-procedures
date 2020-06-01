package apoc.export.json;

import apoc.export.util.ExportConfig;
import com.fasterxml.jackson.core.JsonGenerator;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static apoc.export.util.FormatUtils.getLabelsSorted;

public enum JsonFormatSerializer {

    DEFAULT() {

        @Override
        public void writeNode(JsonGenerator jsonGenerator, Node node, ExportConfig config) throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("type", "node");
            writeNodeDetails(jsonGenerator, node, true);
            jsonGenerator.writeEndObject();
        }

        @Override
        public void writeRelationship(JsonGenerator jsonGenerator, Relationship rel, ExportConfig config) throws IOException {
            Node startNode = rel.getStartNode();
            Node endNode = rel.getEndNode();
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("id", String.valueOf(rel.getId()));
            jsonGenerator.writeStringField("type", "relationship");
            jsonGenerator.writeStringField("label", rel.getType().toString());
            serializeProperties(jsonGenerator, rel.getAllProperties());
            writeRelationshipNode(jsonGenerator, "start", startNode, config);
            writeRelationshipNode(jsonGenerator, "end", endNode, config);
            jsonGenerator.writeEndObject();
        }

        @Override
        public void serializeProperties(JsonGenerator jsonGenerator, Map<String, Object> properties) throws IOException {
            if(properties != null && !properties.isEmpty()) {
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
        public void serializeProperty(JsonGenerator jsonGenerator, String key, Object value, boolean writeKey) throws IOException {
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

        private void writeNodeDetails(JsonGenerator jsonGenerator, Node node, boolean withNodeProperties) throws IOException {
            jsonGenerator.writeStringField("id", String.valueOf(node.getId()));

            if (node.getLabels().iterator().hasNext()) {
                jsonGenerator.writeArrayFieldStart("labels");

                List<String> labels = getLabelsSorted(node);
                for (String label : labels) {
                    jsonGenerator.writeString(label);
                }
                jsonGenerator.writeEndArray();
            }
            if (withNodeProperties) {
                serializeProperties(jsonGenerator, node.getAllProperties());
            }
        }

        private void writeRelationshipNode(JsonGenerator jsonGenerator, String type, Node node, ExportConfig config) throws IOException {
            jsonGenerator.writeObjectFieldStart(type);

            writeNodeDetails(jsonGenerator, node, config.writeNodeProperties());
            jsonGenerator.writeEndObject();
        }
    };

    public abstract void writeNode(JsonGenerator jsonGenerator, Node node, ExportConfig config) throws IOException;

    public abstract void writeRelationship(JsonGenerator jsonGenerator, Relationship relationship, ExportConfig config) throws IOException;

    public abstract void serializeProperties(JsonGenerator jsonGenerator, Map<String,Object> properties) throws IOException;

    public abstract void serializeProperty(JsonGenerator jsonGenerator, String key, Object value, boolean writeKey) throws IOException;

}
