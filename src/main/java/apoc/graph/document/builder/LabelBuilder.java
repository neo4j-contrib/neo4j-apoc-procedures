package apoc.graph.document.builder;

import apoc.graph.util.GraphsConfig;
import org.neo4j.graphdb.Label;

import java.util.Map;

import static org.apache.commons.text.WordUtils.capitalizeFully;

public class LabelBuilder {

    private GraphsConfig config;

    public LabelBuilder(GraphsConfig config) {
        this.config = config;
    }

    public Label buildLabel(Map<String, Object> obj) {
        String label = "DocNode"; // Default label

        Object type = obj.get(config.getLabelField());
        if (type != null) {
            label = String.valueOf(type);
            label = capitalizeFully(label, '_', ' ')
                    .replaceAll("_", "")
                    .replaceAll(" ", "");
        }

        return Label.label(label);
    }

}
