package apoc.custom;

import apoc.ApocConfiguration;
import apoc.util.JsonUtil;
import apoc.util.Util;
import org.neo4j.collection.PrefetchingRawIterator;
import org.neo4j.collection.RawIterator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.*;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.CallableUserFunction;
import org.neo4j.kernel.api.proc.Key;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.core.GraphProperties;
import org.neo4j.kernel.impl.core.GraphPropertiesProxy;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.util.Util.map;
import static java.util.Collections.singletonList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.*;

/**
 * @author mh
 * @since 18.08.18
 */
public class CypherProcedures {

    private static final String PREFIX = "custom";
    public static final String FUNCTIONS = "functions";
    public static final String FUNCTION = "function";
    public static final String PROCEDURES = "procedures";
    public static final String PROCEDURE = "procedure";
    @Context
    public GraphDatabaseAPI api;
    @Context
    public KernelTransaction ktx;
    @Context
    public Log log;

    /*
     * store in graph properties, load at startup
     * allow to register proper params as procedure-params
     * allow to register proper return columns
     * allow to register mode
     */
    @Procedure(value = "apoc.custom.asProcedure",mode = Mode.WRITE)
    @Description("apoc.custom.asProcedure(name, statement, mode, outputs, inputs, description) - register a custom cypher procedure")
    public void asProcedure(@Name("name") String name, @Name("statement") String statement,
                            @Name(value = "mode",defaultValue = "read") String mode,
                            @Name(value= "outputs", defaultValue = "null") List<List<String>> outputs,
                            @Name(value= "inputs", defaultValue = "null") List<List<String>> inputs,
                            @Name(value= "description", defaultValue = "null") String description
    ) throws ProcedureException {
        debug(name,"before", ktx);

        CustomStatementRegistry registry = new CustomStatementRegistry(api, log);
        if (!registry.registerProcedure(name, statement, mode, outputs, inputs, description)) {
            throw new IllegalStateException("Error registering procedure "+name+", see log.");
        }
        CustomProcedureStorage.storeProcedure(api, name, statement, mode, outputs, inputs, description);
        debug(name, "after", ktx);
    }

    public static void debug(@Name("name") String name, String msg, KernelTransaction ktx) {
        try {
            org.neo4j.internal.kernel.api.Procedures procedures = ktx.procedures();
            // ProcedureHandle procedureHandle = procedures.procedureGet(CustomStatementRegistry.qualifiedName(name));
            // if (procedureHandle != null) System.out.printf("%s name: %s id %d%n", msg, procedureHandle.signature().name().toString(), procedureHandle.id());
        } catch (Exception e) {
        }
    }

    @Procedure(value = "apoc.custom.asFunction",mode = Mode.WRITE)
    @Description("apoc.custom.asFunction(name, statement, outputs, inputs, forceSingle, description) - register a custom cypher function")
    public void asFunction(@Name("name") String name, @Name("statement") String statement,
                           @Name(value= "outputs", defaultValue = "") String output,
                           @Name(value= "inputs", defaultValue = "null") List<List<String>> inputs,
                           @Name(value = "forceSingle", defaultValue = "false") boolean forceSingle,
                           @Name(value = "description", defaultValue = "null") String description) throws ProcedureException {
        CustomStatementRegistry registry = new CustomStatementRegistry(api, log);
        if (!registry.registerFunction(name, statement, output, inputs, forceSingle, description)) {
            throw new IllegalStateException("Error registering function "+name+", see log.");
        }
        CustomProcedureStorage.storeFunction(api, name, statement, output, inputs, forceSingle, description);
    }

    @Procedure(value = "apoc.custom.list", mode = Mode.READ)
    @Description("apoc.custom.list() - provide a list of custom procedures/function registered")
    public Stream<CustomProcedureInfo> list(){
        CustomProcedureStorage registry = new CustomProcedureStorage(api, log);
        return registry.list().stream();
    }

    static class CustomStatementRegistry {
        GraphDatabaseAPI api;
        Procedures procedures;
        private final Log log;

        public CustomStatementRegistry(GraphDatabaseAPI api, Log log) {
            this.api = api;
            procedures = api.getDependencyResolver().resolveDependency(Procedures.class);
            this.log = log;
        }

        public boolean registerProcedure(@Name("name") String name, @Name("statement") String statement, @Name(value = "mode", defaultValue = "read") String mode, @Name(value = "outputs", defaultValue = "null") List<List<String>> outputs, @Name(value = "inputs", defaultValue = "null") List<List<String>> inputs, @Name(value= "description", defaultValue = "") String description) {
            try {
                Procedures procedures = api.getDependencyResolver().resolveDependency(Procedures.class);
                ProcedureSignature signature = new ProcedureSignature(qualifiedName(name), inputSignatures(inputs), outputSignatures(outputs),
                        Mode.valueOf(mode.toUpperCase()), null, new String[0], description, null, false, true
                );
                procedures.register(new CallableProcedure.BasicProcedure(signature) {
                    @Override
                    public RawIterator<Object[], ProcedureException> apply(org.neo4j.kernel.api.proc.Context ctx, Object[] input, ResourceTracker resourceTracker) throws ProcedureException {
                        KernelTransaction ktx = ctx.get(Key.key("KernelTransaction", KernelTransaction.class));
                        debug(name, "inside", ktx);
                        Map<String, Object> params = params(input, inputs);
                        Result result = api.execute(statement, params);
                        resourceTracker.registerCloseableResource(result);
                        String[] names = outputs == null ? null : outputs.stream().map(pair -> pair.get(0)).toArray(String[]::new);
                        return new PrefetchingRawIterator<Object[], ProcedureException>() {
                            @Override
                            protected Object[] fetchNextOrNull() {
                                if (!result.hasNext()) return null;
                                Map<String, Object> row = result.next();
                                return toResult(row, names);
                            }
                        };
                    }
                }, true);
                return true;
            } catch (Exception e) {
                log.error("Could not register procedure: " + name + " with " + statement + "\n accepting" + inputs + " resulting in " + outputs + " mode " + mode,e);
                return false;
            }

        }

        public boolean registerFunction(String name, String statement, String output, List<List<String>> inputs, boolean forceSingle, String description)  {
            try {
                AnyType outType = typeof(output.isEmpty() ? "LIST OF MAP" : output);
                UserFunctionSignature signature = new UserFunctionSignature(qualifiedName(name), inputSignatures(inputs), outType,
                        null, new String[0], description, false);

                DefaultValueMapper defaultValueMapper = new DefaultValueMapper(api.getDependencyResolver().resolveDependency(GraphDatabaseFacade.class));

                procedures.register(new CallableUserFunction.BasicUserFunction(signature) {
                    @Override
                    public AnyValue apply(org.neo4j.kernel.api.proc.Context ctx, AnyValue[] input) throws ProcedureException {
                        Map<String, Object> params = functionParams(input, inputs, defaultValueMapper);
                        try (Result result = api.execute(statement, params)) {
//                resourceTracker.registerCloseableResource(result); // TODO
                            if (!result.hasNext()) return null;
                            if (output.isEmpty()) {
                                return ValueUtils.of(result.stream().collect(Collectors.toList()));
                            }
                            List<String> cols = result.columns();
                            if (cols.isEmpty()) return null;
                            if (!forceSingle && outType instanceof ListType) {
                                ListType listType = (ListType) outType;
                                AnyType innerType = listType.innerType();
                                if (innerType instanceof MapType)
                                    return ValueUtils.of(result.stream().collect(Collectors.toList()));
                                if (cols.size() == 1)
                                    return ValueUtils.of(result.stream().map(row -> row.get(cols.get(0))).collect(Collectors.toList()));
                            } else {
                                Map<String, Object> row = result.next();
                                if (outType instanceof MapType) return ValueUtils.of(row);
                                if (cols.size() == 1) return ValueUtils.of(row.get(cols.get(0)));
                            }
                            throw new IllegalStateException("Result mismatch " + cols + " output type is " + output);
                        }
                    }
                }, true);
                return true;
            } catch(Exception e) {
                log.error("Could not register function: "+name+" with "+statement+"\n accepting"+inputs+" resulting in "+output+" single result "+forceSingle,e);
                return false;
            }
        }


        public static QualifiedName qualifiedName(@Name("name") String name) {
            String[] names = name.split("\\.");
            List<String> namespace = new ArrayList<>(names.length);
            namespace.add(PREFIX);
            namespace.addAll(Arrays.asList(names));
            return new QualifiedName(namespace.subList(0,namespace.size()-1), names[names.length-1]);
        }

        public List<FieldSignature> inputSignatures(@Name(value = "inputs", defaultValue = "null") List<List<String>> inputs) {
            List<FieldSignature> inputSignature = inputs == null ? singletonList(FieldSignature.inputField("params", NTMap, DefaultParameterValue.ntMap(Collections.emptyMap()))) :
                    inputs.stream().map(pair -> {
                        DefaultParameterValue defaultValue = defaultValue(pair.get(1), pair.size() > 2 ? pair.get(2) : null);
                        return defaultValue == null ?
                                FieldSignature.inputField(pair.get(0), typeof(pair.get(1))) :
                                FieldSignature.inputField(pair.get(0), typeof(pair.get(1)), defaultValue);
                    }).collect(Collectors.toList());
            ;
            return inputSignature;
        }

        public List<FieldSignature> outputSignatures(@Name(value = "outputs", defaultValue = "null") List<List<String>> outputs) {
            return outputs == null ?  singletonList(FieldSignature.inputField("row", NTMap)) :
                    outputs.stream().map(pair -> FieldSignature.outputField(pair.get(0),typeof(pair.get(1)))).collect(Collectors.toList());
        }

        private Neo4jTypes.AnyType typeof(String typeName) {
            typeName = typeName.toUpperCase();
            if (typeName.startsWith("LIST OF ")) return NTList(typeof(typeName.substring(8)));
            if (typeName.startsWith("LIST ")) return NTList(typeof(typeName.substring(5)));
            switch (typeName) {
                case "ANY": return NTAny;
                case "MAP": return NTMap;
                case "NODE": return NTNode;
                case "REL": return NTRelationship;
                case "RELATIONSHIP": return NTRelationship;
                case "EDGE": return NTRelationship;
                case "PATH": return NTPath;
                case "NUMBER": return NTNumber;
                case "LONG": return NTInteger;
                case "INT": return NTInteger;
                case "INTEGER": return NTInteger;
                case "FLOAT": return NTFloat;
                case "DOUBLE": return NTFloat;
                case "BOOL": return NTBoolean;
                case "BOOLEAN": return NTBoolean;
                case "DATE": return NTDate;
                case "TIME": return NTTime;
                case "LOCALTIME": return NTLocalTime;
                case "DATETIME": return NTDateTime;
                case "LOCALDATETIME": return NTLocalDateTime;
                case "DURATION": return NTDuration;
                case "POINT": return NTPoint;
                case "GEO": return NTGeometry;
                case "GEOMETRY": return NTGeometry;
                case "STRING": return NTString;
                case "TEXT": return NTString;
                default: return NTString;
            }
        }
        private DefaultParameterValue defaultValue(String typeName, String stringValue) {
            if (stringValue == null) return null;
            Object value = JsonUtil.parse(stringValue, null, Object.class);
            if (value == null) return null;
            typeName = typeName.toUpperCase();
            if (typeName.startsWith("LIST ")) return DefaultParameterValue.ntList((List<?>) value,typeof(typeName.substring(5)));
            switch (typeName) {
                case "MAP": return DefaultParameterValue.ntMap((Map<String, Object>) value);
                case "NODE":
                case "REL":
                case "RELATIONSHIP":
                case "EDGE":
                case "PATH": return null;
                case "NUMBER": return value instanceof Float || value instanceof Double ? DefaultParameterValue.ntFloat(((Number)value).doubleValue()) : DefaultParameterValue.ntInteger(((Number)value).longValue());
                case "LONG":
                case "INT":
                case "INTEGER": return DefaultParameterValue.ntInteger(((Number)value).longValue());
                case "FLOAT":
                case "DOUBLE": return DefaultParameterValue.ntFloat(((Number)value).doubleValue());
                case "BOOL":
                case "BOOLEAN": return DefaultParameterValue.ntBoolean((Boolean)value);
                case "DATE":
                case "TIME":
                case "LOCALTIME":
                case "DATETIME":
                case "LOCALDATETIME":
                case "DURATION":
                case "POINT":
                case "GEO":
                case "GEOMETRY": return null;
                case "STRING":
                case "TEXT": return DefaultParameterValue.ntString(value.toString());
                default: return null;
            }
        }
        private Object[] toResult(Map<String, Object> row, String[] names) {
            if (names == null) return new Object[] {row};
            Object[] result = new Object[names.length];
            for (int i = 0; i < names.length; i++) {
                result[i] = row.get(names[i]);
            }
            return result;
        }

        public Map<String, Object> params(Object[] input, @Name(value = "inputs", defaultValue = "null") List<List<String>> inputs) {
            if (inputs == null) return (Map<String,Object>)input[0];
            Map<String, Object> params = new HashMap<>(input.length);
            for (int i = 0; i < input.length; i++) {
                params.put(inputs.get(i).get(0), input[i]);
            }
            return params;
        }

        public Map<String, Object> functionParams(Object[] input, @Name(value = "inputs", defaultValue = "null") List<List<String>> inputs, DefaultValueMapper mapper) {
            if (inputs == null) return (Map<String, Object>)((MapValue)input[0]).map(mapper);
            Map<String, Object> params = new HashMap<>(input.length);
            for (int i = 0; i < input.length; i++) {
                params.put(inputs.get(i).get(0), ((AnyValue)input[i]).map(mapper));
            }
            return params;
        }
    }

    public static class CustomProcedureInfo {
        public String type;
        public String name;
        public String description;
        public String mode;
        public String statement;
        public List<List<String>>inputs;
        public Object outputs;
        public Boolean forceSingle;

        public CustomProcedureInfo(String type, String name, String description, String mode,
                                   String statement, List<List<String>> inputs, Object outputs,
                                   Boolean forceSingle){
            this.type = type;
            this.name = name;
            this.description = description;
            this.statement = statement;
            this.outputs = outputs;
            this.inputs = inputs;
            this.forceSingle = forceSingle;
            this.mode = mode;
        }
    }

    public static class CustomProcedureStorage implements AvailabilityGuard.AvailabilityListener {
        public static final String APOC_CUSTOM = "apoc.custom";
        public static final String APOC_CUSTOM_UPDATE = "apoc.custom.update";
        private GraphProperties properties;
        private final GraphDatabaseAPI api;
        private final Log log;
        private Timer timer = new Timer(getClass().getSimpleName(), true);
        private long lastUpdate;

        public CustomProcedureStorage(GraphDatabaseAPI api, Log log) {
            this.api = api;
            this.log = log;
        }

        @Override
        public void available() {
            properties = getProperties(api);
            restoreProcedures();
            long refreshInterval = Long.valueOf(ApocConfiguration.get("custom.procedures.refresh", "60000"));
            timer.scheduleAtFixedRate(new TimerTask() {
                public void run() {
                    restoreProcedures();
                }
            }, refreshInterval, refreshInterval);
        }

        public static GraphPropertiesProxy getProperties(GraphDatabaseAPI api) {
            return api.getDependencyResolver().resolveDependency(EmbeddedProxySPI.class).newGraphPropertiesProxy();
        }

        private void restoreProcedures() {
            if (getLastUpdate(properties) <= lastUpdate) return;
            lastUpdate = System.currentTimeMillis();
            CustomStatementRegistry registry = new CustomStatementRegistry(api, log);
            Map<String, Map<String, Map<String, Object>>> stored = readData(properties);
            stored.get(FUNCTIONS).forEach((name, data) -> {
                registry.registerFunction(name, (String) data.get("statement"), (String) data.get("output"),
                        (List<List<String>>) data.get("inputs"), (Boolean) data.get("forceSingle"), (String) data.get("description"));
            });
            stored.get(PROCEDURES).forEach((name, data) -> {
                registry.registerProcedure(name, (String) data.get("statement"), (String) data.get("mode"),
                        (List<List<String>>) data.get("outputs"), (List<List<String>>) data.get("inputs"), (String) data.get("description"));
            });
            clearQueryCaches(api);
        }

        @Override
        public void unavailable() {
            if (timer != null) {
                timer.cancel();
            }
            properties = null;
        }

        public static Map<String, Object> storeProcedure(GraphDatabaseAPI api, String name, String statement, String mode, List<List<String>> outputs, List<List<String>> inputs, String description) {

            Map<String, Object> data = map("statement", statement, "mode", mode, "inputs", inputs, "outputs", outputs, "description", description);
            return updateCustomData(getProperties(api), name, PROCEDURES, data);
        }
        public static Map<String, Object> storeFunction(GraphDatabaseAPI api, String name, String statement, String output, List<List<String>> inputs, boolean forceSingle, String description) {
            Map<String, Object> data = map("statement", statement, "forceSingle", forceSingle, "inputs", inputs, "output", output, "description", description);
            return updateCustomData(getProperties(api), name, FUNCTIONS, data);
        }

        public synchronized static Map<String, Object> remove(GraphDatabaseAPI api, String name, String type) {
            return updateCustomData(getProperties(api),name, type,null);
        }

        private synchronized static Map<String, Object> updateCustomData(GraphProperties properties, String name, String type, Map<String, Object> value) {
            if (name == null || type==null) return null;
            try (Transaction tx = properties.getGraphDatabase().beginTx()) {
                Map<String, Map<String, Map<String, Object>>> data = readData(properties);
                Map<String, Map<String, Object>> procData = data.get(type);
                Map<String, Object> previous = (value == null) ? procData.remove(name) : procData.put(name, value);
                if (value != null || previous != null) {
                    properties.setProperty(APOC_CUSTOM, Util.toJson(data));
                    properties.setProperty(APOC_CUSTOM_UPDATE, System.currentTimeMillis());
                }
                tx.success();
                return previous;
            }
        }

        private static long getLastUpdate(GraphProperties properties) {
            try (Transaction tx = properties.getGraphDatabase().beginTx()) {
                long lastUpdate = (long) properties.getProperty(APOC_CUSTOM_UPDATE, 0L);
                tx.success();
                return lastUpdate;
            }
        }
        private static Map<String, Map<String,Map<String, Object>>> readData(GraphProperties properties) {
            try (Transaction tx = properties.getGraphDatabase().beginTx()) {
                String procedurePropertyData = (String) properties.getProperty(APOC_CUSTOM, "{\"functions\":{},\"procedures\":{}}");
                Map result = Util.fromJson(procedurePropertyData, Map.class);
                tx.success();
                return result;
            }
        }

        private static void clearQueryCaches(GraphDatabaseService db) {
            try (Transaction tx = db.beginTx()) {
                db.execute("call dbms.clearQueryCaches()").close();
                tx.success();
            }
        }


        public List<CustomProcedureInfo> list() {
            return readData(getProperties(api)).entrySet().stream()
                    .flatMap(entryProcedureType -> {
                        Map<String, Map<String, Object>> procedures = entryProcedureType.getValue();
                        String type = entryProcedureType.getKey();
                        boolean isProcedure = PROCEDURES.equals(type);
                        return procedures.entrySet().stream().map(entryProcedure -> {
                            String typeLabel = isProcedure ? PROCEDURE : FUNCTION;
                            String outputs = isProcedure ? "outputs" : "output";
                            String procedureName = entryProcedure.getKey();
                            Map<String, Object> procedureParams = entryProcedure.getValue();
                            return new CustomProcedureInfo(typeLabel, procedureName,
                                    "null".equals(procedureParams.get("description")) ?
                                            null : String.valueOf(procedureParams.get("description")),
                                    procedureParams.containsKey("mode")
                                            ? String.valueOf(procedureParams.get("mode")) : null,
                                    String.valueOf(procedureParams.get("statement")),
                                    (List<List<String>>) procedureParams.get("inputs"),
                                    procedureParams.get(outputs),
                                    (Boolean) procedureParams.get("forceSingle"));
                        });
                    })
                    .collect(Collectors.toList());
        }
    }
}