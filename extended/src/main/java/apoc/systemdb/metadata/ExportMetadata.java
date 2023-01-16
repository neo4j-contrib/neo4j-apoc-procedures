package apoc.systemdb.metadata;

import apoc.ExtendedSystemLabels;
import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.export.util.ProgressReporter;
import apoc.systemdb.SystemDbConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import java.util.List;
import java.util.Optional;


public interface ExportMetadata {

    enum Type {
        CypherProcedure(new ExportProcedure()),
        CypherFunction(new ExportFunction()),
        Uuid(new ExportUuid()),
        Trigger(new ExportTrigger()),
        DataVirtualizationCatalog(new ExportDataVirtualization());

        private final ExportMetadata exportMetadata;

        Type(ExportMetadata exportMetadata) {
            this.exportMetadata = exportMetadata;
        }

        public List<Pair<String, String>> export(Node node, ProgressReporter progressReporter) {
            return exportMetadata.export(node, progressReporter);
        }

        public static Optional<Type> from(Label label, SystemDbConfig config) {
            final String name = label.name();
            if (name.equalsIgnoreCase( ExtendedSystemLabels.Procedure.name())) {
                return get(CypherProcedure, config);
            } else if(name.equalsIgnoreCase(ExtendedSystemLabels.Function.name())) {
                return get(CypherFunction, config);
            } else if(name.equalsIgnoreCase(SystemLabels.ApocTrigger.name())) {
                return get(Trigger, config);
            } else if(name.equalsIgnoreCase(ExtendedSystemLabels.ApocUuid.name())) {
                return get(Uuid, config);
            } else if(name.equalsIgnoreCase(ExtendedSystemLabels.DataVirtualizationCatalog.name())) {
                return get(DataVirtualizationCatalog, config);
            }
            return Optional.empty();
        }

        private static Optional<Type> get(Type cypherProcedure, SystemDbConfig config) {
            return config.getFeatures().contains(cypherProcedure.name())
                    ? Optional.of(cypherProcedure)
                    : Optional.empty();
        }
    }

    List<Pair<String, String>> export(Node node, ProgressReporter progressReporter);

    default String getFileName(Node node, String prefix) {
        // we create a file featureName.dbName because there could be features coming from different databases
        String dbName = (String) node.getProperty(SystemPropertyKeys.database.name(), null);
        dbName = StringUtils.isEmpty(dbName) ? StringUtils.EMPTY : "." + dbName;
        return prefix + dbName;
    }
}