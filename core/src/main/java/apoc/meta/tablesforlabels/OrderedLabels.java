package apoc.meta.tablesforlabels;

import apoc.meta.Tables4LabelsProfile;
import org.neo4j.graphdb.Label;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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
        return o instanceof OrderedLabels && nodeLabels().equals(((OrderedLabels)o).nodeLabels());
    }

    public String asNodeType() {
        return ":" + labels.stream()
                .map(s -> "`" + s + "`")
                .collect(Collectors.joining( ":" ));
    }

    public List<String> nodeLabels() { return labels; }
}
