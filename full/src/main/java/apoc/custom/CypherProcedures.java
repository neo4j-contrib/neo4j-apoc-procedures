/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.custom;

import static apoc.custom.CypherProceduresHandler.*;
import static apoc.util.SystemDbUtil.checkWriteAllowed;
import static apoc.util.Util.getAllQueryProcs;
import static org.neo4j.graphdb.QueryExecutionType.QueryType;

import apoc.Extended;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

/**
 * @author mh
 * @since 18.08.18
 */
@Extended
public class CypherProcedures {
    private static final String MSG_DEPRECATION = "Please note that the current procedure is deprecated,\n"
            + "it's recommended to use the `apoc.custom.installProcedure`, `apoc.custom.installFunction`, `apoc.uuid.dropProcedure` , `apoc.uuid.dropFunction` , `apoc.uuid.dropAll` procedures executed against the 'system' database\n"
            + "instead of, respectively, `apoc.uuid.declareProcedure`, `apoc.uuid.declareFunction`, `apoc.custom.removeProcedure`, `apoc.custom.removeFunction`, `apoc.custom.removeAll`.";

    // visible for testing
    public static final String ERROR_MISMATCHED_INPUTS =
            "Required query parameters do not match provided input arguments.";
    public static final String ERROR_MISMATCHED_OUTPUTS = "Query results do not match requested output.";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public KernelTransaction ktx;

    @Context
    public Log log;

    @Context
    public CypherProceduresHandler cypherProceduresHandler;

    /*
     * store in graph properties, load at startup
     * allow to register proper params as procedure-params
     * allow to register proper return columns
     * allow to register mode
     */
    @Procedure(value = "apoc.custom.asProcedure", mode = Mode.WRITE, deprecatedBy = "apoc.custom.declareProcedure")
    @Description(
            "apoc.custom.asProcedure(name, statement, mode, outputs, inputs, description) - register a custom cypher procedure")
    @Deprecated
    public void asProcedure(
            @Name("name") String name,
            @Name("statement") String statement,
            @Name(value = "mode", defaultValue = "read") String mode,
            @Name(value = "outputs", defaultValue = "null") List<List<String>> outputs,
            @Name(value = "inputs", defaultValue = "null") List<List<String>> inputs,
            @Name(value = "description", defaultValue = "") String description)
            throws ProcedureException {
        checkWriteAllowed(api, MSG_DEPRECATION);
        ProcedureSignature signature =
                cypherProceduresHandler.procedureSignature(name, mode, outputs, inputs, description);
        Mode modeProcedure = cypherProceduresHandler.mode(mode);
        validateProcedure(statement, signature.inputSignature(), signature.outputSignature(), modeProcedure);
        cypherProceduresHandler.storeProcedure(signature, statement);
    }

    @Deprecated
    @Procedure(value = "apoc.custom.declareProcedure", mode = Mode.WRITE, deprecatedBy = "apoc.custom.installProcedure")
    @Description(
            "apoc.custom.declareProcedure(signature, statement, mode, description) - register a custom cypher procedure")
    public void declareProcedure(
            @Name("signature") String signature,
            @Name("statement") String statement,
            @Name(value = "mode", defaultValue = "read") String mode,
            @Name(value = "description", defaultValue = "") String description) {
        checkWriteAllowed(api, MSG_DEPRECATION);
        Mode modeProcedure = CypherProceduresUtil.mode(mode);
        ProcedureSignature procedureSignature =
                new Signatures(PREFIX).asProcedureSignature(signature, description, modeProcedure);
        validateProcedure(
                statement, procedureSignature.inputSignature(), procedureSignature.outputSignature(), modeProcedure);

        cypherProceduresHandler.storeProcedure(procedureSignature, statement);
    }

    @Procedure(value = "apoc.custom.asFunction", mode = Mode.WRITE, deprecatedBy = "apoc.custom.declareFunction")
    @Description(
            "apoc.custom.asFunction(name, statement, outputs, inputs, forceSingle, description) - register a custom cypher function")
    @Deprecated
    public void asFunction(
            @Name("name") String name,
            @Name("statement") String statement,
            @Name(value = "outputs", defaultValue = "") String output,
            @Name(value = "inputs", defaultValue = "null") List<List<String>> inputs,
            @Name(value = "forceSingle", defaultValue = "false") boolean forceSingle,
            @Name(value = "description", defaultValue = "") String description)
            throws ProcedureException {
        UserFunctionSignature signature = cypherProceduresHandler.functionSignature(name, output, inputs, description);
        validateFunction(statement, signature.inputSignature());
        cypherProceduresHandler.storeFunction(signature, statement, forceSingle, false);
    }

    @Deprecated
    @Procedure(value = "apoc.custom.declareFunction", mode = Mode.WRITE, deprecatedBy = "apoc.custom.installFunction")
    @Description(
            "apoc.custom.declareFunction(signature, statement, forceSingle, description) - register a custom cypher function")
    public void declareFunction(
            @Name("signature") String signature,
            @Name("statement") String statement,
            @Name(value = "forceSingle", defaultValue = "false") boolean forceSingle,
            @Name(value = "description", defaultValue = "") String description)
            throws ProcedureException {
        checkWriteAllowed(api, MSG_DEPRECATION);
        UserFunctionSignature userFunctionSignature =
                new Signatures(PREFIX).asFunctionSignature(signature, description);
        final Signatures signatures = new Signatures(PREFIX);
        final SignatureParser.FunctionContext functionContext = signatures.parseFunction(signature);
        validateFunction(statement, userFunctionSignature.inputSignature());
        final boolean mapResult = signatures.isMapResult(functionContext);

        cypherProceduresHandler.storeFunction(userFunctionSignature, statement, forceSingle, mapResult);
    }

    @Procedure(value = "apoc.custom.list", mode = Mode.READ)
    @Description("apoc.custom.list() - provide a list of custom procedures/function registered")
    public Stream<CustomProcedureInfo> list() {
        return cypherProceduresHandler.readSignatures().map(descriptor -> {
            String statement = descriptor.getStatement();
            if (descriptor instanceof CypherProceduresHandler.ProcedureDescriptor) {
                CypherProceduresHandler.ProcedureDescriptor procedureDescriptor =
                        (CypherProceduresHandler.ProcedureDescriptor) descriptor;
                ProcedureSignature signature = procedureDescriptor.getSignature();
                return CustomProcedureInfo.getCustomProcedureInfo(signature, statement);
            } else {
                CypherProceduresHandler.UserFunctionDescriptor userFunctionDescriptor =
                        (CypherProceduresHandler.UserFunctionDescriptor) descriptor;
                UserFunctionSignature signature = userFunctionDescriptor.getSignature();
                return CustomProcedureInfo.getCustomFunctionInfo(
                        signature, userFunctionDescriptor.isForceSingle(), statement);
            }
        });
    }

    @Deprecated
    @Procedure(value = "apoc.custom.removeProcedure", mode = Mode.WRITE, deprecatedBy = "apoc.custom.dropProcedure")
    @Description("apoc.custom.removeProcedure(name) - remove the targeted custom procedure")
    public void removeProcedure(@Name("name") String name) {
        checkWriteAllowed(api, MSG_DEPRECATION);
        cypherProceduresHandler.removeProcedure(name);
    }

    @Deprecated
    @Procedure(value = "apoc.custom.removeFunction", mode = Mode.WRITE, deprecatedBy = "apoc.custom.dropFunction")
    @Description("apoc.custom.removeFunction(name, type) - remove the targeted custom function")
    public void removeFunction(@Name("name") String name) {
        checkWriteAllowed(api, MSG_DEPRECATION);
        cypherProceduresHandler.removeFunction(name);
    }

    private void validateFunction(String statement, List<FieldSignature> input) {
        validateProcedure(statement, input, DEFAULT_MAP_OUTPUT, null);
    }

    private void validateProcedure(
            String statement, List<FieldSignature> input, List<FieldSignature> output, Mode mode) {

        final Set<String> outputSet = output.stream().map(FieldSignature::name).collect(Collectors.toSet());

        api.executeTransactionally(
                "EXPLAIN " + statement,
                input.stream().collect(HashMap::new, (map, value) -> map.put(value.name(), null), HashMap::putAll),
                result -> {
                    if (!DEFAULT_MAP_OUTPUT.equals(output)) {
                        // when there are multiple variables with the same name, e.g within an "UNION ALL" Neo4j adds a
                        // suffix "@<number>" to distinguish them,
                        //  so to check the correctness of the output parameters we must first remove this suffix from
                        // the column names
                        final Set<String> columns = result.columns().stream()
                                .map(i -> i.replaceFirst("@[0-9]+", "").trim())
                                .collect(Collectors.toSet());
                        checkOutputParams(outputSet, columns);
                    }
                    if (!DEFAULT_INPUTS.equals(input)) {
                        checkInputParams(result);
                    }
                    if (mode != null) {
                        checkMode(result, mode);
                    }
                    return null;
                });
    }

    private void checkMode(Result result, Mode mode) {
        Set<Mode> modes = new HashSet<>() {
            {
                // parameter mode
                add(mode);
                // all modes can have DEFAULT and READ procedures as well
                add(Mode.DEFAULT);
                add(Mode.READ);
            }
        };

        // schema can have WRITE procedures as well
        if (mode.equals(Mode.SCHEMA)) {
            modes.add(Mode.WRITE);
        }

        // check that all inner procedure have a correct Mode
        if (!procsAreValid(api, modes, result)) {
            throw new RuntimeException(
                    "One or more inner procedure modes have operation different from the mode parameter: " + mode);
        }

        // check that the `Result.getQueryExecutionType()` is correct
        checkCorrectQueryType(result, mode);
    }

    private void checkCorrectQueryType(Result result, Mode mode) {
        List<QueryType> readQueryTypes = List.of(QueryType.READ_ONLY);
        List<QueryType> writeQueryTypes = List.of(QueryType.READ_ONLY, QueryType.WRITE, QueryType.READ_WRITE);
        List<QueryType> schemaQueryTypes =
                List.of(QueryType.READ_ONLY, QueryType.WRITE, QueryType.READ_WRITE, QueryType.SCHEMA_WRITE);
        List<QueryType> dbmsQueryTypes = List.of(QueryType.READ_ONLY, QueryType.DBMS);

        // create a map of Mode to allowed `QueryType`s
        // WRITE mode can have READ and WRITE query types
        // SCHEMA mode can have SCHEMA, READ and WRITE query types
        // DBMS mode can have READ and DBMS query types
        Map<Mode, List<QueryType>> modeQueryTypeMap = Map.of(
                Mode.READ, readQueryTypes,
                Mode.WRITE, writeQueryTypes,
                Mode.SCHEMA, schemaQueryTypes,
                Mode.DBMS, dbmsQueryTypes);

        List<QueryType> queryTypes = modeQueryTypeMap.get(mode);

        // check that the statement have a valid queryType
        QueryType queryType = isQueryValid(result, queryTypes.toArray(QueryType[]::new));
        // if query type not matched
        if (queryType != null) {
            /*
            The `correspondenceList` prints a list like:
                - Mode: SCHEMA can have as a query execution type: [READ_ONLY, WRITE, READ_WRITE, SCHEMA_WRITE]
                - Mode: DBMS can have as a query execution type: [DBMS]
                ...
             */
            String correspondenceList = modeQueryTypeMap.entrySet().stream()
                    .map(i -> "- Mode: " + i.getKey() + " can have as a query execution type: " + i.getValue())
                    .collect(Collectors.joining("\n"));

            throw new RuntimeException(String.format(
                    "The query execution type of the statement is: `%s`, but you provided as a parameter mode: `%s`.\n"
                            + "You have to declare a `mode` which corresponds to one of the following query execution type.\n"
                            + "That is:\n"
                            + "%s",
                    queryType.name(), mode.name(), correspondenceList));
        }
    }

    // Similar to `boolean isQueryTypeValid` located in Util.java (APOC Core)
    public static QueryExecutionType.QueryType isQueryValid(
            Result result, QueryExecutionType.QueryType[] supportedQueryTypes) {
        QueryExecutionType.QueryType type = result.getQueryExecutionType().queryType();
        // if everything is ok return null, otherwise the current getQueryExecutionType().queryType()
        if (supportedQueryTypes != null
                && supportedQueryTypes.length != 0
                && Stream.of(supportedQueryTypes).noneMatch(sqt -> sqt.equals(type))) {
            return type;
        }
        return null;
    }

    public static boolean procsAreValid(GraphDatabaseService db, Set<Mode> supportedModes, Result result) {
        if (supportedModes != null && !supportedModes.isEmpty()) {
            final ExecutionPlanDescription executionPlanDescription = result.getExecutionPlanDescription();
            // get procedures used in the query
            Set<String> queryProcNames = new HashSet<>();
            getAllQueryProcs(executionPlanDescription, queryProcNames);

            if (!queryProcNames.isEmpty()) {
                final Set<String> modes =
                        supportedModes.stream().map(Mode::name).collect(Collectors.toSet());
                // check if sub-procedures have valid mode
                final Set<String> procNames = db.executeTransactionally(
                        "SHOW PROCEDURES YIELD name, mode where mode in $modes return name",
                        Map.of("modes", modes),
                        r -> Iterators.asSet(r.columnAs("name")));

                return procNames.containsAll(queryProcNames);
            }
        }

        return true;
    }

    private void checkOutputParams(Set<String> outputSet, Set<String> columns) {
        if (!Set.copyOf(columns).equals(outputSet)) {
            throw new RuntimeException(ERROR_MISMATCHED_OUTPUTS);
        }
    }

    private void checkInputParams(Result result) {
        String missingParameters = StreamSupport.stream(
                        result.getNotifications().spliterator(), false)
                .filter(i -> i.getCode()
                        .equals(Status.Statement.ParameterMissing.code().serialize()))
                .map(Notification::getDescription)
                .collect(Collectors.joining(System.lineSeparator()));

        if (StringUtils.isNotBlank(missingParameters)) {
            throw new RuntimeException(ERROR_MISMATCHED_INPUTS);
        }
    }
}
