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

public class ExportDataVirtualization implements ExportMetadata {

    @Override
    public List<Pair<String, String>> export(Node node, ProgressReporterExtended progressReporter) {
        final String dvName = (String) node.getProperty(ExtendedSystemPropertyKeys.name.name());
        try {
            final String data = toCypherMap(JsonUtilExtended.OBJECT_MAPPER.readValue((String) node.getProperty( ExtendedSystemPropertyKeys.data.name()), Map.class));
            final String statement = String.format("CALL apoc.dv.catalog.add('%s', %s)", dvName, data);
            progressReporter.nextRow();
            return List.of(Pair.of(getFileName(node, Type.DataVirtualizationCatalog.name()), statement));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}