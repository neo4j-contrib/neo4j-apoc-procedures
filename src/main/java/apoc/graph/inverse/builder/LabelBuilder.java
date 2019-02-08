package apoc.graph.inverse.builder;

import org.neo4j.graphdb.Label;

import java.util.Map;

import static org.apache.commons.text.WordUtils.capitalizeFully;

public class LabelBuilder {

    public Label buildLabel(Map<String, Object> obj) {
        String label = "DocNode"; // Default label

        Object type = obj.get("type");
        if (type != null) {
            label = String.valueOf(type);
            label = capitalizeFully(label, '_', ' ')
                    .replaceAll("_", "")
                    .replaceAll(" ", "");
        }

        return Label.label(label);
    }

}
