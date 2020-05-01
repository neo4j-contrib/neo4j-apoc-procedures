package apoc.graph.document.builder;

import apoc.graph.util.GraphsConfig;
import apoc.text.Strings;
import org.neo4j.graphdb.Label;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.commons.text.WordUtils.capitalize;
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
