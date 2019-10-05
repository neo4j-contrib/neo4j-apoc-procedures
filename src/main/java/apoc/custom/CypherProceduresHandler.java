package apoc.custom;

import apoc.ApocConfig;
import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.JsonUtil;
import apoc.util.Util;
import org.neo4j.collection.RawIterator;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.graphdb.*;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.*;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.CallableUserFunction;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.impl.ComponentRegistry;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.ApocConfig.apocConfig;
import static java.util.Collections.singletonList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.*;

public class CypherProceduresHandler implements AvailabilityListener {

    private static final String PREFIX = "custom";
    public static final String FUNCTION = "function";
    public static final String PROCEDURE = "procedure";

    private static final Map<String, CypherProceduresHandler> cypherProceduresLifecyclesByDatabaseName = new ConcurrentHashMap<>();

    public static final String CUSTOM_PROCEDURES_REFRESH = "apoc.custom.procedures.refresh";
    private final GraphDatabaseAPI api;
    private final Log log;
    private final GraphDatabaseService systemDb;
    private final GlobalProceduresRegistry globalProceduresRegistry;
    private Timer timer = new Timer(getClass().getSimpleName(), true);
    private long lastUpdate;
    private final ThrowingFunction<Context, Transaction, ProcedureException> transactionComponentFunction;


    public CypherProceduresHandler(GraphDatabaseAPI db, DatabaseManagementService databaseManagementService, ApocConfig apocConfig, Log userLog, GlobalProceduresRegistry globalProceduresRegistry) {
        this.api = db;
        this.log = userLog;
        this.systemDb = apocConfig.getSystemDb();
        this.globalProceduresRegistry = globalProceduresRegistry;
        cypherProceduresLifecyclesByDatabaseName.put(db.databaseName(), this);
        globalProceduresRegistry.registerComponent(CypherProceduresHandler.class, ctx -> {
            String databaseName = ctx.graphDatabaseAPI().databaseName();
            return cypherProceduresLifecyclesByDatabaseName.get(databaseName);
        }, true);

        // FIXME: remove reflection once fixed upstream
        try {
            Field field = globalProceduresRegistry.getClass().getDeclaredField("safeComponents");
            field.setAccessible(true);
            Method providerFor = ComponentRegistry.class.getDeclaredMethod("providerFor", Class.class);
            providerFor.setAccessible(true);
            ComponentRegistry safeComponents = (ComponentRegistry) field.get(globalProceduresRegistry);
            transactionComponentFunction = (ThrowingFunction<Context, Transaction, ProcedureException>) providerFor.invoke(safeComponents, Transaction.class);
        } catch (NoSuchFieldException|NoSuchMethodException|IllegalAccessException| InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void available() {
        restoreProcedures();
        long refreshInterval = apocConfig().getInt(CUSTOM_PROCEDURES_REFRESH, 60000);
        timer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                try {
                    if (getLastUpdate() > lastUpdate) {
                        restoreProcedures();
                    }
                } catch (DatabaseShutdownException e) {
                    // ignore
                }
            }
        }, refreshInterval, refreshInterval);
    }

    private void restoreProcedures() {
        lastUpdate = System.currentTimeMillis();
        try (Transaction tx = systemDb.beginTx()) {
            final ValueMapper valueMapper = new DefaultValueMapper((InternalTransaction) tx);
            tx.findNodes(SystemLabels.ApocCypherProcedures, SystemPropertyKeys.database.name(), api.databaseName())
                .forEachRemaining(node -> {

                    String name = (String) node.getProperty(SystemPropertyKeys.name.name());
                    String statement = (String) node.getProperty(SystemPropertyKeys.statement.name());
                    List<List<String>> inputs = Util.fromJson((String) node.getProperty(SystemPropertyKeys.inputs.name()), List.class);
                    String description = (String) node.getProperty(SystemPropertyKeys.description.name(), null);
                    if (node.hasLabel(SystemLabels.Procedure)) {
                        String mode = (String) node.getProperty(SystemPropertyKeys.mode.name(), null);
                        List<List<String>> outputs = Util.fromJson((String) node.getProperty(SystemPropertyKeys.outputs.name()), List.class);
                        registerProcedure(valueMapper, name, statement, mode, outputs, inputs, description);
                    } else if (node.hasLabel(SystemLabels.Function)) {
                        String output = (String) node.getProperty(SystemPropertyKeys.output.name(), null);
                        boolean forceSingle = (boolean) node.getProperty(SystemPropertyKeys.forceSingle.name(), false);
                        registerFunction((InternalTransaction) tx, name, statement, output, inputs, forceSingle, description);
                    }
                });

            tx.commit();
        }
        clearQueryCaches(api);
    }

    @Override
    public void unavailable() {
        if (timer != null) {
            timer.cancel();
        }
    }

    public static Node mergeNode(Transaction tx, Label primaryLabel, Label addtionalLabel,
                                 String key1, Object value1, String key2, Object value2) {
        Node node = Iterators.singleOrNull(tx.findNodes(primaryLabel, key1, value1, key2, value2).stream().filter(n -> n.hasLabel(addtionalLabel)).iterator());
        if (node==null) {
            node = tx.createNode(primaryLabel, addtionalLabel);
            node.setProperty(key1, value1);
            node.setProperty(key2, value2);
        }
        return node;
    }

    public void storeProcedure(ValueMapper valueMapper, String name, String statement, String mode, List<List<String>> outputs, List<List<String>> inputs, String description) {
        store(SystemLabels.Procedure, name, statement, inputs, description, (tx, node) -> {
            node.setProperty(SystemPropertyKeys.mode.name(), mode);
            node.setProperty(SystemPropertyKeys.outputs.name(), Util.toJson(outputs));
            registerProcedure(valueMapper, name, statement, mode, outputs, inputs, description);
        });
    }

    public void storeFunction(String name, String statement, String output, List<List<String>> inputs, boolean forceSingle, String description) {
        store(SystemLabels.Function, name, statement, inputs, description, (tx, node) -> {
            node.setProperty(SystemPropertyKeys.output.name(), output);
            node.setProperty(SystemPropertyKeys.forceSingle.name(), forceSingle);
            registerFunction((InternalTransaction) tx, name, statement, output, inputs, forceSingle, description);
        });
    }

    private void store(Label secondLabel, String name, String statement, List<List<String>> inputs, String description, BiConsumer<Transaction, Node> typeSpecific) {
        try (Transaction tx = systemDb.beginTx()) {
            Node node = mergeNode(tx, SystemLabels.ApocCypherProcedures, secondLabel,
                    SystemPropertyKeys.database.name(), api.databaseName(),
                    SystemPropertyKeys.name.name(), name);

            if (!"null".equals(description)) {
                node.setProperty(SystemPropertyKeys.description.name(), description);
            }
            node.setProperty(SystemPropertyKeys.inputs.name(), Util.toJson(inputs));
            node.setProperty(SystemPropertyKeys.statement.name(), statement);
            setLastUpdate(tx);
            typeSpecific.accept(tx, node);
            tx.commit();
        }
    }

    /*public synchronized static Map<String, Object> remove(GraphDatabaseAPI api, String name, String type) {
        return updateCustomData(getProperties(api), name, type, null);
    }

    private synchronized static Map<String, Object> updateCustomData(GraphProperties properties, String name, String type, Map<String, Object> value) {
        if (name == null || type == null) return null;
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
    }*/

    private void setLastUpdate(Transaction tx) {
        Node node = tx.findNode(SystemLabels.ApocCypherProceduresMeta, SystemPropertyKeys.database.name(), api.databaseName());
        if (node == null) {
            node = tx.createNode(SystemLabels.ApocCypherProceduresMeta);
            node.setProperty(SystemPropertyKeys.database.name(), api.databaseName());
        }
        node.setProperty(SystemPropertyKeys.lastUpdated.name(), System.currentTimeMillis());
    }

    private long getLastUpdate() {
        try (Transaction tx = systemDb.beginTx()) {
            Node node = tx.findNode(SystemLabels.ApocCypherProceduresMeta, SystemPropertyKeys.database.name(), api.databaseName());
            long retVal = node == null ? 0L : (long) node.getProperty(SystemPropertyKeys.lastUpdated.name());
            tx.commit();
            return retVal;
        }
   }

    private void clearQueryCaches(GraphDatabaseService db) {
        db.executeTransactionally("call dbms.clearQueryCaches()");
    }

    public List<Map<String, Object>> list() {
        try (Transaction tx = systemDb.beginTx()) {
            List<Map<String, Object>> list = tx.findNodes(SystemLabels.ApocCypherProcedures, SystemPropertyKeys.database.name(), api.databaseName())
                    .stream()
                    .<Map<String, Object>>map(node -> {
                        HashMap<String, Object> map = new HashMap<>(node.getAllProperties());
                        map.put("type", node.hasLabel(SystemLabels.Procedure) ? PROCEDURE : FUNCTION);
                        map.put("inputs", Util.fromJson((String) map.get("inputs"), List.class));

                        map.put("output",
                                map.containsKey("outputs") ?
                                        Util.fromJson((String) map.get("outputs"), List.class) :
                                        map.get("output"));
                        return map;
                    }).collect(Collectors.toList());
            tx.commit();
            return list;
        }
    }

    /*public List<CypherProcedures.CustomProcedureInfo> list() {
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
                        return new CypherProcedures.CustomProcedureInfo(typeLabel, procedureName,
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
    }*/

    public boolean registerProcedure(ValueMapper valueMapper, String name, String statement, String mode, List<List<String>> outputs,  List<List<String>> inputs,  String description) {
        try {
            boolean admin = false; // TODO
            ProcedureSignature signature = new ProcedureSignature(qualifiedName(name), inputSignatures(inputs), outputSignatures(outputs),
                    Mode.valueOf(mode.toUpperCase()), admin, null, new String[0], description, null,  false, false, true
            );
            globalProceduresRegistry.register(new CallableProcedure.BasicProcedure(signature) {
                @Override
                public RawIterator<AnyValue[], ProcedureException> apply(org.neo4j.kernel.api.procedure.Context ctx, AnyValue[] input, ResourceTracker resourceTracker) throws ProcedureException {
//                    KernelTransaction ktx = ctx.kernelTransaction();
//                    debug(name, "inside", ktx);
                    Map<String, Object> params = params(input, inputs, valueMapper);

                    Transaction tx = transactionComponentFunction.apply(ctx);
                    Result result = tx.execute(statement, params);
                    resourceTracker.registerCloseableResource(result);
                    String[] names = outputs == null ? null : outputs.stream().map(pair -> pair.get(0)).toArray(String[]::new);

                    Stream<AnyValue[]> stream = result.stream().map(row -> toResult(row, names));
                    return Iterators.asRawIterator(stream);

                }
            }, true);
            return true;
        } catch (Exception e) {
            log.error("Could not register procedure: " + name + " with " + statement + "\n accepting" + inputs + " resulting in " + outputs + " mode " + mode,e);
            return false;
        }

    }

    public boolean registerFunction(InternalTransaction internalTransaction, String name, String statement, String output, List<List<String>> inputs, boolean forceSingle, String description)  {
        try {
            Neo4jTypes.AnyType outType = typeof(output.isEmpty() ? "LIST OF MAP" : output);
            UserFunctionSignature signature = new UserFunctionSignature(qualifiedName(name), inputSignatures(inputs), outType,
                    null, new String[0], description, false);

            DefaultValueMapper defaultValueMapper = new DefaultValueMapper(internalTransaction);

            globalProceduresRegistry.register(new CallableUserFunction.BasicUserFunction(signature) {
                @Override
                public AnyValue apply(org.neo4j.kernel.api.procedure.Context ctx, AnyValue[] input) throws ProcedureException {
                    Map<String, Object> params = functionParams(input, inputs, defaultValueMapper);

                    Transaction tx = transactionComponentFunction.apply(ctx);
                    try (Result result = tx.execute(statement, params)) {
//                resourceTracker.registerCloseableResource(result); // TODO
                        if (!result.hasNext()) return null;
                        if (output.isEmpty()) {
                            return ValueUtils.of(result.stream().collect(Collectors.toList()));
                        }
                        List<String> cols = result.columns();
                        if (cols.isEmpty()) return null;
                        if (!forceSingle && outType instanceof Neo4jTypes.ListType) {
                            Neo4jTypes.ListType listType = (Neo4jTypes.ListType) outType;
                            Neo4jTypes.AnyType innerType = listType.innerType();
                            if (innerType instanceof Neo4jTypes.MapType)
                                return ValueUtils.of(result.stream().collect(Collectors.toList()));
                            if (cols.size() == 1)
                                return ValueUtils.of(result.stream().map(row -> row.get(cols.get(0))).collect(Collectors.toList()));
                        } else {
                            Map<String, Object> row = result.next();
                            if (outType instanceof Neo4jTypes.MapType) return ValueUtils.of(row);
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

    private AnyValue[] toResult(Map<String, Object> row, String[] names) {
        if (names == null) {
            return new AnyValue[]{convertToValueRecursive(row)};
        } else {
            AnyValue[] result = new AnyValue[names.length];
            for (int i = 0; i < names.length; i++) {
                result[i] = convertToValueRecursive(row.get(names[i]));
            }
            return result;
        }
    }

    private AnyValue convertToValueRecursive(Object... toConverts) {
        switch (toConverts.length) {
            case 0:
                return Values.NO_VALUE;
            case 1:
                Object toConvert = toConverts[0];
                if (toConvert instanceof List) {
                    List list = (List) toConvert;
                    AnyValue[] objects = ((Stream<AnyValue>) list.stream().map(x -> convertToValueRecursive(x))).toArray(AnyValue[]::new);
                    return VirtualValues.list(objects);
                } else if (toConvert instanceof Map) {
                    Map<String,Object> map = (Map) toConvert;
                    MapValueBuilder builder = new MapValueBuilder(map.size());
                    map.entrySet().stream().forEach(e ->{
                        builder.add(e.getKey(), convertToValueRecursive(e.getValue()));
                    });
                    return builder.build();
                } else {
                    return Values.of(toConvert);
                }
            default:
                AnyValue[] values = Arrays.stream(toConverts).map(c -> convertToValueRecursive(c)).toArray(AnyValue[]::new);
                return VirtualValues.list(values);
        }
    }

    public Map<String, Object> params(AnyValue[] input, @Name(value = "inputs", defaultValue = "null") List<List<String>> inputs, ValueMapper valueMapper) {
        if (inputs == null) return (Map<String, Object>) input[0].map(valueMapper);
        Map<String, Object> params = new HashMap<>(input.length);
        for (int i = 0; i < input.length; i++) {
            params.put(inputs.get(i).get(0), input[i].map(valueMapper));
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
