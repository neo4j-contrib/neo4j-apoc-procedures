package apoc.systemdb.metadata;

import apoc.SystemPropertyKeys;
import apoc.export.util.ProgressReporter;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static apoc.util.Util.toCypherMap;

public class ExportUuid implements ExportMetadata {

    @Override
    public List<Pair<String, String>> export(Node node, ProgressReporter progressReporter) {
        Map<String, Object> map = new HashMap<>();
        final String labelName = (String) node.getProperty(SystemPropertyKeys.label.name());
        final String property = (String) node.getProperty(SystemPropertyKeys.propertyName.name());
        map.put("addToSetLabels", node.getProperty(SystemPropertyKeys.addToSetLabel.name(), null));
        map.put("uuidProperty", property);
        final String uuidConfig = toCypherMap(map);
        // add constraint - TODO: might be worth add config to export or not this file
        String schemaStatement = String.format("CREATE CONSTRAINT ON (n:%s) ASSERT n.%s IS UNIQUE;\n", labelName, property);
        final String statement = String.format("CALL apoc.uuid.install('%s', %s);", labelName, uuidConfig);
        progressReporter.nextRow();
        return List.of(
                Pair.of(getFileName(node, Type.Uuid.name() + ".schema"), schemaStatement),
                Pair.of(getFileName(node, Type.Uuid.name()), statement)
        );

    }
}
