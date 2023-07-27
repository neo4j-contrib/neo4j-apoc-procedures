package apoc.systemdb.metadata;

import apoc.ExtendedSystemPropertyKeys;
import apoc.SystemPropertyKeys;
import apoc.custom.CypherHandlerNewProcedure;
import apoc.custom.CypherProceduresUtil;
import apoc.export.util.ProgressReporter;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.procs.FieldSignature;

import java.util.List;
import java.util.stream.Collectors;

public class ExportProcedure implements ExportMetadata {

    @Override
    public List<Pair<String, String>> export(Node node, ProgressReporter progressReporter) {
        final String inputs = getSignature(node, ExtendedSystemPropertyKeys.inputs.name());

        final String outputName = ExtendedSystemPropertyKeys.output.name();
        final String outputs = node.hasProperty(outputName)
                ? (String) node.getProperty(outputName)
                : getSignature(node, ExtendedSystemPropertyKeys.outputs.name());

        String statement = String.format("CALL apoc.custom.declareProcedure('%s(%s) :: (%s)', '%s', '%s', '%s');",
                node.getProperty(SystemPropertyKeys.name.name()), inputs, outputs,
                node.getProperty(SystemPropertyKeys.statement.name()),
                node.getProperty(ExtendedSystemPropertyKeys.mode.name()),
                node.getProperty(ExtendedSystemPropertyKeys.description.name()));
        progressReporter.nextRow();
        return List.of(Pair.of(getFileName(node, Type.CypherProcedure.name()), statement));
    }


    static String getSignature(Node node, String name) {
        return CypherProceduresUtil.deserializeSignatures((String) node.getProperty(name))
                .stream().map(FieldSignature::toString)
                .collect(Collectors.joining(", "));
    }
}