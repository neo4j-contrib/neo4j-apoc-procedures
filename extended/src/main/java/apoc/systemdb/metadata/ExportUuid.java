package apoc.systemdb.metadata;

import apoc.ExtendedSystemPropertyKeys;
import apoc.export.util.ProgressReporter;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.Node;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static apoc.util.ExtendedUtil.toCypherMap;

public class ExportUuid implements ExportMetadata {

    @Override
    public List<Pair<String, String>> export(Node node, ProgressReporter progressReporter) {
        Map<String, Object> map = new HashMap<>();
        final String labelName = (String) node.getProperty( ExtendedSystemPropertyKeys.label.name());
        final String property = (String) node.getProperty(ExtendedSystemPropertyKeys.propertyName.name());
        map.put("addToSetLabels", node.getProperty(ExtendedSystemPropertyKeys.addToSetLabel.name(), null));
        map.put("uuidProperty", property);
        final String uuidConfig = toCypherMap(map);
        // add constraint - TODO: might be worth add config to export or not this file
        String schemaStatement = String.format("CREATE CONSTRAINT %1$s_%2$s IF NOT EXISTS FOR (n:%1$s) REQUIRE n.%2$s IS UNIQUE;\n", labelName, property);
        final String statement = String.format("CALL apoc.uuid.install('%s', %s);", labelName, uuidConfig);
        progressReporter.nextRow();
        return List.of(
                Pair.of(getFileName(node, Type.Uuid.name() + ".schema"), schemaStatement),
                Pair.of(getFileName(node, Type.Uuid.name()), statement)
        );

    }
}
