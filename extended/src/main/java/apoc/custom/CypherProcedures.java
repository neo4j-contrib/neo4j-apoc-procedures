package apoc.custom;

import apoc.Extended;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Mode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.custom.CypherProceduresHandler.*;
import static apoc.util.SystemDbUtil.checkWriteAllowed;

/**
 * @author mh
 * @since 18.08.18
 */
@Extended
public class CypherProcedures {
    private static final String MSG_DEPRECATION = """
            Please note that the current procedure is deprecated,
            it's recommended to use the `apoc.custom.installProcedure`, `apoc.custom.installFunction`, `apoc.uuid.dropProcedure` , `apoc.uuid.dropFunction` , `apoc.uuid.dropAll` procedures executed against the 'system' database
            instead of, respectively, `apoc.uuid.declareProcedure`, `apoc.uuid.declareFunction`, `apoc.custom.removeProcedure`, `apoc.custom.removeFunction`, `apoc.custom.removeAll`.""";

    // visible for testing
    public static final String ERROR_MISMATCHED_INPUTS = "Required query parameters do not match provided input arguments.";
    public static final String ERROR_MISMATCHED_OUTPUTS = "Query results do not match requested output.";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public CypherProceduresHandler cypherProceduresHandler;

    @Deprecated
    @Procedure(value = "apoc.custom.declareProcedure", mode = Mode.WRITE, deprecatedBy = "apoc.custom.installProcedure")
    @Description("apoc.custom.declareProcedure(signature, statement, mode, description) - register a custom cypher procedure")
    public void declareProcedure(@Name("signature") String signature, @Name("statement") String statement,
                                 @Name(value = "mode", defaultValue = "read") String mode,
                                 @Name(value = "description", defaultValue = "") String description
    ) {
        checkWriteAllowed(MSG_DEPRECATION);
        Mode modeProcedure = CypherProceduresUtil.mode(mode);
        ProcedureSignature procedureSignature = new Signatures(PREFIX).asProcedureSignature(signature, description, modeProcedure);
        validateProcedure(statement, procedureSignature.inputSignature(), procedureSignature.outputSignature(), modeProcedure);

        cypherProceduresHandler.storeProcedure(procedureSignature, statement);
    }

    @Deprecated
    @Procedure(value = "apoc.custom.declareFunction", mode = Mode.WRITE, deprecatedBy = "apoc.custom.installFunction")
    @Description("apoc.custom.declareFunction(signature, statement, forceSingle, description) - register a custom cypher function")
    public void declareFunction(@Name("signature") String signature, @Name("statement") String statement,
                           @Name(value = "forceSingle", defaultValue = "false") boolean forceSingle,
                           @Name(value = "description", defaultValue = "") String description) throws ProcedureException {
        checkWriteAllowed(MSG_DEPRECATION);
        UserFunctionSignature userFunctionSignature = new Signatures(PREFIX).asFunctionSignature(signature, description);
        final Signatures signatures = new Signatures(PREFIX);
        final SignatureParser.FunctionContext functionContext = signatures.parseFunction(signature);
        validateFunction(statement, userFunctionSignature.inputSignature());
        final boolean mapResult = signatures.isMapResult(functionContext);

        cypherProceduresHandler.storeFunction(userFunctionSignature, statement, forceSingle, mapResult);
    }


    @Procedure(value = "apoc.custom.list", mode = Mode.READ)
    @Description("apoc.custom.list() - provide a list of custom procedures/function registered")
    public Stream<CustomProcedureInfo> list() {
        return cypherProceduresHandler.readSignatures().map( descriptor -> {
            String statement = descriptor.getStatement();
            if (descriptor instanceof CypherProceduresHandler.ProcedureDescriptor) {
                CypherProceduresHandler.ProcedureDescriptor procedureDescriptor = (CypherProceduresHandler.ProcedureDescriptor) descriptor;
                ProcedureSignature signature = procedureDescriptor.getSignature();
                return CustomProcedureInfo.getCustomProcedureInfo(signature, statement);
            } else {
                CypherProceduresHandler.UserFunctionDescriptor userFunctionDescriptor = (CypherProceduresHandler.UserFunctionDescriptor) descriptor;
                UserFunctionSignature signature = userFunctionDescriptor.getSignature();
                return CustomProcedureInfo.getCustomFunctionInfo(signature, userFunctionDescriptor.isForceSingle(), statement);
            }
        });
    }
    

    @Deprecated
    @Procedure(value = "apoc.custom.removeProcedure", mode = Mode.WRITE, deprecatedBy = "apoc.custom.dropProcedure")
    @Description("apoc.custom.removeProcedure(name) - remove the targeted custom procedure")
    public void removeProcedure(@Name("name") String name) {
        checkWriteAllowed(MSG_DEPRECATION);
        cypherProceduresHandler.removeProcedure(name);
    }


    @Deprecated
    @Procedure(value = "apoc.custom.removeFunction", mode = Mode.WRITE, deprecatedBy = "apoc.custom.dropFunction")
    @Description("apoc.custom.removeFunction(name, type) - remove the targeted custom function")
    public void removeFunction(@Name("name") String name) {
        checkWriteAllowed(MSG_DEPRECATION);
        cypherProceduresHandler.removeFunction(name);
    }

    private void validateFunction(String statement, List<FieldSignature> input) {
        validateProcedure(statement, input, DEFAULT_MAP_OUTPUT, null);
    }

    private void validateProcedure(String statement, List<FieldSignature> input, List<FieldSignature> output, Mode mode) {

        final Set<String> outputSet = output.stream().map(FieldSignature::name).collect(Collectors.toSet());

        api.executeTransactionally("EXPLAIN " + statement,
                input.stream().collect(HashMap::new,
                                (map, value) -> map.put(value.name(), null), HashMap::putAll),
                result -> {
                    if (!DEFAULT_MAP_OUTPUT.equals(output)) {
                        // when there are multiple variables with the same name, e.g within an "UNION ALL" Neo4j adds a suffix "@<number>" to distinguish them,
                        //  so to check the correctness of the output parameters we must first remove this suffix from the column names
                        final Set<String> columns = result.columns().stream()
                                .map(i -> i.replaceFirst("@[0-9]+", "").trim())
                                .collect(Collectors.toSet());
                        checkOutputParams(outputSet, columns);
                    }
                    if (!DEFAULT_INPUTS.equals(input)) {
                        checkInputParams(result);
                    }
                    if (mode != null) {
                        checkMode(result.getQueryExecutionType().queryType(), mode);
                    }
                    return null;
                });
    }

    private void checkMode(QueryExecutionType.QueryType queryType, Mode mode) {
        Map<QueryExecutionType.QueryType, Mode> map = Map.of(QueryExecutionType.QueryType.WRITE, Mode.WRITE,
                QueryExecutionType.QueryType.READ_ONLY, Mode.READ,
                QueryExecutionType.QueryType.READ_WRITE, Mode.WRITE,
                QueryExecutionType.QueryType.DBMS, Mode.DBMS,
                QueryExecutionType.QueryType.SCHEMA_WRITE, Mode.SCHEMA);

        if (!map.get(queryType).equals(mode)) {
            throw new RuntimeException(String.format("The query execution type is %s, but you provided mode %s.\n" +
                            "Supported modes are %s",
                    queryType.name(),
                    mode.name(),
                    map.values().stream().sorted().collect(Collectors.toList())));
        }
    }


    private void checkOutputParams(Set<String> outputSet, Set<String> columns) {
        if (!Set.copyOf(columns).equals(outputSet)) {
            throw new RuntimeException(ERROR_MISMATCHED_OUTPUTS);
        }
    }

    private void checkInputParams(Result result) {
        String missingParameters = StreamSupport.stream(result.getNotifications().spliterator(), false)
                .filter(i -> i.getCode().equals(Status.Statement.ParameterMissing.code().serialize()))
                .map(Notification::getDescription)
                .collect(Collectors.joining(System.lineSeparator()));

        if (StringUtils.isNotBlank(missingParameters)) {
            throw new RuntimeException(ERROR_MISMATCHED_INPUTS);
        }
    }

}
