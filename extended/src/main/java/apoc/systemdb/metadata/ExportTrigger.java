package apoc.systemdb.metadata;

import apoc.SystemPropertyKeys;
import apoc.export.util.ProgressReporter;
import apoc.util.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.Node;

import java.util.List;
import java.util.Map;

import static apoc.util.ExtendedUtil.toCypherMap;

public class ExportTrigger implements ExportMetadata {

    @Override
    public List<Pair<String, String>> export(Node node, ProgressReporter progressReporter) {
        final String name = (String) node.getProperty(SystemPropertyKeys.name.name());
        final String query = (String) node.getProperty(SystemPropertyKeys.statement.name());
        try {
            final String selector = toCypherMap(JsonUtil.OBJECT_MAPPER.readValue((String) node.getProperty(SystemPropertyKeys.selector.name()), Map.class));
            final String params = toCypherMap(JsonUtil.OBJECT_MAPPER.readValue((String) node.getProperty(SystemPropertyKeys.params.name()), Map.class));
            String statement = String.format("CALL apoc.trigger.add('%s', '%s', %s, {params: %s});", name, query, selector, params);
            if ((boolean) node.getProperty(SystemPropertyKeys.paused.name())) {
                statement += String.format("\nCALL apoc.trigger.pause('%s');", name);
            }
            progressReporter.nextRow();
            return List.of(Pair.of(getFileName(node, Type.Trigger.name()), statement));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}