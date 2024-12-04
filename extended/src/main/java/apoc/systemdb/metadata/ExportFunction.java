package apoc.systemdb.metadata;

import apoc.ExtendedSystemPropertyKeys;
import apoc.custom.CypherProceduresUtil;
import apoc.export.util.ProgressReporterExtended;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.procs.FieldSignature;

import java.util.List;
import java.util.stream.Collectors;


public class ExportFunction implements ExportMetadata {

    @Override
    public List<Pair<String, String>> export(Node node, ProgressReporterExtended progressReporter) {
        final String inputs = getSignature(node, ExtendedSystemPropertyKeys.inputs.name());

        final String outputName = ExtendedSystemPropertyKeys.output.name();
        final String outputs = node.hasProperty(outputName)
                ? (String) node.getProperty(outputName)
                : getSignature(node, ExtendedSystemPropertyKeys.outputs.name());

        String statement = String.format("CALL apoc.custom.declareFunction('%s(%s) :: %s', '%s', %s, '%s');",
                node.getProperty(ExtendedSystemPropertyKeys.name.name()), inputs, outputs,
                node.getProperty(ExtendedSystemPropertyKeys.statement.name()),
                node.getProperty(ExtendedSystemPropertyKeys.forceSingle.name()),
                node.getProperty(ExtendedSystemPropertyKeys.description.name()));
        progressReporter.nextRow();
        return List.of(Pair.of(getFileName(node, Type.CypherFunction.name()), statement));
    }


    static String getSignature(Node node, String name) {
        return CypherProceduresUtil.deserializeSignatures((String) node.getProperty(name))
                .stream().map(FieldSignature::toString)
                .collect(Collectors.joining(", "));
    }
}