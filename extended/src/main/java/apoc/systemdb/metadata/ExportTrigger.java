package apoc.systemdb.metadata;

import apoc.ExtendedSystemPropertyKeys;
import apoc.export.util.ProgressReporterExtended;
import apoc.util.JsonUtilExtended;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.Node;

import java.util.List;
import java.util.Map;

import static apoc.util.ExtendedUtil.toCypherMap;

public class ExportTrigger implements ExportMetadata {

    @Override
    public List<Pair<String, String>> export(Node node, ProgressReporterExtended progressReporter) {
        final String name = (String) node.getProperty(ExtendedSystemPropertyKeys.name.name());
        final String query = (String) node.getProperty(ExtendedSystemPropertyKeys.statement.name());
        try {
            final String selector = toCypherMap(JsonUtilExtended.OBJECT_MAPPER.readValue((String) node.getProperty(ExtendedSystemPropertyKeys.selector.name()), Map.class));
            final String params = toCypherMap(JsonUtilExtended.OBJECT_MAPPER.readValue((String) node.getProperty(ExtendedSystemPropertyKeys.params.name()), Map.class));
            String statement = String.format("CALL apoc.trigger.add('%s', '%s', %s, {params: %s});", name, query, selector, params);
            if ((boolean) node.getProperty(ExtendedSystemPropertyKeys.paused.name())) {
                statement += String.format("\nCALL apoc.trigger.pause('%s');", name);
            }
            progressReporter.nextRow();
            return List.of(Pair.of(getFileName(node, Type.Trigger.name()), statement));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}