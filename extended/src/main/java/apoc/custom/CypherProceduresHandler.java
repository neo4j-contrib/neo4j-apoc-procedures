package apoc.custom;

import apoc.ApocConfig;
import apoc.ExtendedSystemLabels;
import apoc.ExtendedSystemPropertyKeys;
import apoc.SystemPropertyKeys;
import apoc.util.JsonUtil;
import apoc.util.Util;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.collection.RawIterator;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.CallableUserFunction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.impl.ProcedureHolderUtils;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.ApocConfig.apocConfig;
import static apoc.custom.CypherProceduresUtil.*;
import static apoc.custom.CypherHandlerNewProcedure.serializeSignatures;
import static java.util.Collections.singletonList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.AnyType;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTAny;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTMap;

public class CypherProceduresHandler extends LifecycleAdapter implements AvailabilityListener {
    public static final String PREFIX = "custom";
    public static final String FUNCTION = "function";
    public static final String PROCEDURE = "procedure";
    public static final String CUSTOM_PROCEDURES_REFRESH = "apoc.custom.procedures.refresh";
    public static final List<FieldSignature> DEFAULT_INPUTS = singletonList(FieldSignature.inputField("params", NTMap, DefaultParameterValue.ntMap(Collections.emptyMap())));
    public static final List<FieldSignature> DEFAULT_MAP_OUTPUT = singletonList(FieldSignature.inputField("row", NTMap));
    public static final String ERROR_INVALID_TYPE = "Invalid type name." +
            "\nCheck the documentation to see possible values: https://neo4j.com/labs/apoc/4.1/cypher-execution/cypher-based-procedures-functions/";

    private final GraphDatabaseAPI api;
    private final Log log;
    private final GraphDatabaseService systemDb;
    private final GlobalProcedures globalProceduresRegistry;
    private final JobScheduler jobScheduler;
    private long lastUpdate;
    private final Set<ProcedureSignature> registeredProcedureSignatures = Collections.synchronizedSet(new HashSet<>());
    private final Set<UserFunctionSignature> registeredUserFunctionSignatures = Collections.synchronizedSet(new HashSet<>());
    private static Group REFRESH_GROUP = Group.STORAGE_MAINTENANCE;
    private JobHandle restoreProceduresHandle;


    public CypherProceduresHandler(GraphDatabaseAPI db, JobScheduler jobScheduler, ApocConfig apocConfig, Log userLog, GlobalProcedures globalProceduresRegistry) {
        this.api = db;
        this.log = userLog;
        this.jobScheduler = jobScheduler;
        this.systemDb = apocConfig.getSystemDb();
        this.globalProceduresRegistry = globalProceduresRegistry;

    }

    @Override
    public void available() {
        restoreProceduresAndFunctions();
        long refreshInterval = apocConfig().getInt(CUSTOM_PROCEDURES_REFRESH, 60000);
        restoreProceduresHandle = jobScheduler.scheduleRecurring(REFRESH_GROUP, () -> {
            if (getLastUpdate() > lastUpdate) {
                restoreProceduresAndFunctions();
            }
        }, refreshInterval, refreshInterval, TimeUnit.MILLISECONDS);
    }

    @Override
    public void unavailable() {
        if (restoreProceduresHandle != null) {
            restoreProceduresHandle.cancel();
        }
    }

    public Stream<ProcedureOrFunctionDescriptor> readSignatures() {
        List<ProcedureOrFunctionDescriptor> descriptors;
        try (Transaction tx = systemDb.beginTx()) {
             descriptors = tx.findNodes( ExtendedSystemLabels.ApocCypherProcedures, SystemPropertyKeys.database.name(), api.databaseName()).stream().map(node -> {
                if (node.hasLabel(ExtendedSystemLabels.Procedure)) {
                    return procedureDescriptor(node);
                } else if (node.hasLabel(ExtendedSystemLabels.Function)) {
                    return userFunctionDescriptor(node);
                } else {
                    throw new IllegalStateException("don't know what to do with systemdb node " + node);
                }
            }).collect(Collectors.toList());
            tx.commit();
        }
        return descriptors.stream();
    }

    private ProcedureDescriptor procedureDescriptor(Node node) {
        String statement = (String) node.getProperty(SystemPropertyKeys.statement.name());

        ProcedureSignature procedureSignature = getProcedureSignature(node);
        return new ProcedureDescriptor(procedureSignature, statement);
    }

    private UserFunctionDescriptor userFunctionDescriptor(Node node) {
        String statement = (String) node.getProperty(SystemPropertyKeys.statement.name());
        boolean forceSingle = (boolean) node.getProperty(ExtendedSystemPropertyKeys.forceSingle.name(), false);
        boolean mapResult = (boolean) node.getProperty(ExtendedSystemPropertyKeys.mapResult.name(), false);

        UserFunctionSignature signature = getUserFunctionSignature(node);
        return new UserFunctionDescriptor(signature, statement, forceSingle, mapResult);
    }

    public synchronized void restoreProceduresAndFunctions() {
        lastUpdate = System.currentTimeMillis();
        Set<ProcedureSignature> currentProceduresToRemove = new HashSet<>(registeredProcedureSignatures);
        Set<UserFunctionSignature> currentUserFunctionsToRemove = new HashSet<>(registeredUserFunctionSignatures);

        readSignatures().forEach(descriptor -> {
            descriptor.register();
            if (descriptor instanceof ProcedureDescriptor) {
                ProcedureSignature signature = ((ProcedureDescriptor) descriptor).getSignature();
                currentProceduresToRemove.remove(signature);
            } else {
                UserFunctionSignature signature = ((UserFunctionDescriptor) descriptor).getSignature();
                currentUserFunctionsToRemove.remove(signature);
            }
        });

        // de-register removed procs/functions
        currentProceduresToRemove.forEach(signature -> registerProcedure(signature, null));
        currentUserFunctionsToRemove.forEach(this::registerFunction);

        api.executeTransactionally("call db.clearQueryCaches()");
    }

    private <T> T withSystemDb(Function<Transaction, T> action) {
        try (Transaction tx = systemDb.beginTx()) {
            T result = action.apply(tx);
            tx.commit();
            return result;
        }
    }

    public synchronized void storeFunction(UserFunctionSignature signature, String statement, boolean forceSingle, boolean mapResult) {
        withSystemDb(tx -> {
            Node node = Util.mergeNode(tx, ExtendedSystemLabels.ApocCypherProcedures, ExtendedSystemLabels.Function,
                    Pair.of(SystemPropertyKeys.database.name(), api.databaseName()),
                    Pair.of(SystemPropertyKeys.name.name(), signature.name().name()),
                    Pair.of(ExtendedSystemPropertyKeys.prefix.name(), signature.name().namespace())
            );
            node.setProperty(ExtendedSystemPropertyKeys.description.name(), signature.description().orElse(null));
            node.setProperty(SystemPropertyKeys.statement.name(), statement);
            node.setProperty(ExtendedSystemPropertyKeys.inputs.name(), serializeSignatures(signature.inputSignature()));
            node.setProperty(ExtendedSystemPropertyKeys.output.name(), signature.outputType().toString());
            node.setProperty(ExtendedSystemPropertyKeys.forceSingle.name(), forceSingle);
            node.setProperty(ExtendedSystemPropertyKeys.mapResult.name(), mapResult);

            setLastUpdate(tx);
            if (!registerFunction(signature, statement, forceSingle, mapResult)) {
                throw new IllegalStateException("Error registering function " + signature + ", see log.");
            }
            return null;
        });
    }

    public synchronized void storeProcedure(ProcedureSignature signature, String statement) {
        withSystemDb(tx -> {
            Node node = Util.mergeNode(tx, ExtendedSystemLabels.ApocCypherProcedures, ExtendedSystemLabels.Procedure,
                    Pair.of(SystemPropertyKeys.database.name(), api.databaseName()),
                    Pair.of(SystemPropertyKeys.name.name(), signature.name().name()),
                    Pair.of(ExtendedSystemPropertyKeys.prefix.name(), signature.name().namespace())
            );
            node.setProperty(ExtendedSystemPropertyKeys.description.name(), signature.description().orElse(null));
            node.setProperty(SystemPropertyKeys.statement.name(), statement);
            node.setProperty(ExtendedSystemPropertyKeys.inputs.name(), serializeSignatures(signature.inputSignature()));
            node.setProperty(ExtendedSystemPropertyKeys.outputs.name(), serializeSignatures(signature.outputSignature()));
            node.setProperty(ExtendedSystemPropertyKeys.mode.name(), signature.mode().name());
            setLastUpdate(tx);
            if (!registerProcedure(signature, statement)) {
                throw new IllegalStateException("Error registering procedure " + signature.name() + ", see log.");
            }
            return null;
        });
    }

    private void setLastUpdate(Transaction tx) {
        Node node = tx.findNode(ExtendedSystemLabels.ApocCypherProceduresMeta, SystemPropertyKeys.database.name(), api.databaseName());
        if (node == null) {
            node = tx.createNode(ExtendedSystemLabels.ApocCypherProceduresMeta);
            node.setProperty(SystemPropertyKeys.database.name(), api.databaseName());
        }
        node.setProperty(SystemPropertyKeys.lastUpdated.name(), System.currentTimeMillis());
    }

    private long getLastUpdate() {
        return withSystemDb( tx -> {
            Node node = tx.findNode(ExtendedSystemLabels.ApocCypherProceduresMeta, SystemPropertyKeys.database.name(), api.databaseName());
            return node == null ? 0L : (long) node.getProperty(SystemPropertyKeys.lastUpdated.name());
        });
    }

    /**
     *
     * @param signature
     * @param statement null indicates a removed procedure
     * @return
     */
    public boolean registerProcedure(ProcedureSignature signature, String statement) {
        QualifiedName name = signature.name();
        try {
            boolean exists = globalProceduresRegistry.getCurrentView().getAllProcedures().stream()
                    .anyMatch(s -> s.name().equals(name));
            if (exists) {
                // we deregister and remove possible homonyms signatures overridden/overloaded
                ProcedureHolderUtils.unregisterProcedure(name, globalProceduresRegistry);
                registeredProcedureSignatures.removeIf(i -> i.name().equals(signature.name()));
            }

            final boolean isStatementNull = statement == null;
            globalProceduresRegistry.register(new CallableProcedure.BasicProcedure(signature) {
                @Override
                public RawIterator<AnyValue[], ProcedureException> apply(org.neo4j.kernel.api.procedure.Context ctx, AnyValue[] input, ResourceMonitor resourceMonitor) throws ProcedureException {
                    if (isStatementNull) {
                        final String error = String.format("There is no procedure with the name `%s` registered for this database instance. " +
                                "Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.", name);
                        throw new QueryExecutionException(error, null, "Neo.ClientError.Statement.SyntaxError");
                    } else {
                        Map<String, Object> params = params(input, signature.inputSignature(), ctx.valueMapper());
                        Transaction tx = ctx.transaction();
                        Result result = tx.execute(statement, params);
                        resourceMonitor.registerCloseableResource(result);

                        List<FieldSignature> outputs = signature.outputSignature();
                        String[] names = outputs == null ? null : outputs.stream().map(FieldSignature::name).toArray(String[]::new);
                        boolean defaultOutputs = outputs == null || outputs.equals(DEFAULT_MAP_OUTPUT);

                        Stream<AnyValue[]> stream = result.stream().map(row -> toResult(row, names, defaultOutputs));
                        return Iterators.asRawIterator(stream);
                    }
                }
            });
            if (isStatementNull) {
                registeredProcedureSignatures.remove(signature);
            } else {
                registeredProcedureSignatures.add(signature);
            }
            return true;
        } catch (Exception e) {
            log.error("Could not register procedure: " + name + " with " + statement + "\n accepting" + signature.inputSignature() + " resulting in " + signature.outputSignature() + " mode " + signature.mode(), e);
            return false;
        }
    }

    public boolean registerFunction(UserFunctionSignature signature) {
        return registerFunction(signature, null, false, false);
    }

    public boolean registerFunction(UserFunctionSignature signature, String statement, boolean forceSingle, boolean mapResult) {
        try {
            QualifiedName name = signature.name();
            boolean exists = globalProceduresRegistry.getCurrentView().getAllNonAggregatingFunctions()
                    .anyMatch(s -> s.name().equals(name));
            if (exists) {
                // we deregister and remove possible homonyms signatures overridden/overloaded
                ProcedureHolderUtils.unregisterFunction(name, globalProceduresRegistry);
                registeredUserFunctionSignatures.removeIf(i -> i.name().equals(signature.name()));
            }

            final boolean isStatementNull = statement == null;
            globalProceduresRegistry.register(new CallableUserFunction.BasicUserFunction(signature) {
                @Override
                public AnyValue apply(org.neo4j.kernel.api.procedure.Context ctx, AnyValue[] input) throws ProcedureException {
                    if (isStatementNull) {
                        final String error = String.format("Unknown function '%s'", name);
                        throw new QueryExecutionException(error, null, "Neo.ClientError.Statement.SyntaxError");
                    } else {
                        Map<String, Object> params = params(input, signature.inputSignature(), ctx.valueMapper());
                        AnyType outType = signature.outputType();
                        Transaction tx = ctx.transaction();
                        try (Result result = tx.execute(statement, params)) {
//                resourceTracker.registerCloseableResource(result); // TODO
                            if (!result.hasNext()) return Values.NO_VALUE;
                            if (outType.equals(NTAny)) {
                                return ValueUtils.of(result.stream().collect(Collectors.toList()));
                            }
                            List<String> cols = result.columns();
                            if (cols.isEmpty()) return null;
                            if (!forceSingle && outType instanceof Neo4jTypes.ListType) {
                                Neo4jTypes.ListType listType = (Neo4jTypes.ListType) outType;
                                AnyType innerType = listType.innerType();
                                if (isWrapped(innerType, mapResult))
                                    return ValueUtils.of(result.stream().collect(Collectors.toList()));
                                if (cols.size() == 1)
                                    return ValueUtils.of(result.stream().map(row -> row.get(cols.get(0))).collect(Collectors.toList()));
                            } else {
                                Map<String, Object> row = result.next();
                                if (isWrapped(outType, mapResult)) {
                                    return ValueUtils.of(row);
                                }
                                if (cols.size() == 1) return ValueUtils.of(row.get(cols.get(0)));
                            }
                            throw new IllegalStateException("Result mismatch " + cols + " output type is " + outType);
                        }
                    }

                }
            });
            if (isStatementNull) {
                registeredUserFunctionSignatures.remove(signature);
            } else {
                registeredUserFunctionSignatures.add(signature);
            }
            return true;
        } catch (Exception e) {
            log.error("Could not register function: " + signature + "\nwith: " + statement + "\n single result " + forceSingle, e);
            return false;
        }
    }

    /**
     * We wrap the result only if we have a "true" map,
     * that is: the output signature is not a `MAP` / `LIST OF MAP` 
     *  and the outputType is exactly equals to MapType 
     *  (neither with NodeType nor with RelationshipType wrap the result, even though they extend MapType)
     */
    private boolean isWrapped(AnyType outType, boolean mapResult) {
        return !mapResult && outType.getClass().equals(Neo4jTypes.MapType.class);
    }

    public static QualifiedName qualifiedName(@Name("name") String name) {
        String[] names = name.split("\\.");
        List<String> namespace = new ArrayList<>(names.length);
        namespace.add(PREFIX);
        namespace.addAll(Arrays.asList(names));
        return new QualifiedName(namespace.subList(0, namespace.size() - 1), names[names.length - 1]);
    }

    private AnyValue[] toResult(Map<String, Object> row, String[] names, boolean defaultOutputs) {
        if (defaultOutputs) {
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
                    Map<String, Object> map = (Map) toConvert;
                    MapValueBuilder builder = new MapValueBuilder();
                    map.entrySet().stream().forEach(e -> {
                        builder.add(e.getKey(), convertToValueRecursive(e.getValue()));
                    });
                    return builder.build();
                } else if (toConvert instanceof Entity || toConvert instanceof Path){
                    return ValueUtils.asAnyValue(toConvert);
                } else {
                    return Values.of(toConvert);
                }
            default:
                AnyValue[] values = Arrays.stream(toConverts).map(c -> convertToValueRecursive(c)).toArray(AnyValue[]::new);
                return VirtualValues.list(values);
        }
    }

    public Map<String, Object> params(AnyValue[] input, List<FieldSignature> fieldSignatures, ValueMapper valueMapper) {
        if (input == null || input.length == 0) return Collections.emptyMap();

        if (fieldSignatures == null || fieldSignatures.isEmpty() || fieldSignatures.equals(DEFAULT_INPUTS))
            return (Map<String, Object>) input[0].map(valueMapper);
        Map<String, Object> params = new HashMap<>(input.length);
        for (int i = 0; i < input.length; i++) {
            params.put(fieldSignatures.get(i).name(), input[i].map(valueMapper));
        }
        return params;
    }

    public void removeProcedure(String name) {
        withSystemDb(tx -> {
            QualifiedName qName = qualifiedName(name);
            tx.findNodes(ExtendedSystemLabels.ApocCypherProcedures,
                    SystemPropertyKeys.database.name(), api.databaseName(),
                    SystemPropertyKeys.name.name(), qName.name(),
                    ExtendedSystemPropertyKeys.prefix.name(), qName.namespace()
            ).stream().filter(n -> n.hasLabel(ExtendedSystemLabels.Procedure)).forEach(node -> {
                ProcedureDescriptor descriptor = procedureDescriptor(node);
                registerProcedure(descriptor.getSignature(), null);
                node.delete();
                setLastUpdate(tx);
            });
            return null;
        });
    }

    public void removeFunction(String name) {
        withSystemDb(tx -> {
            QualifiedName qName = qualifiedName(name);
            tx.findNodes(ExtendedSystemLabels.ApocCypherProcedures,
                    SystemPropertyKeys.database.name(), api.databaseName(),
                    SystemPropertyKeys.name.name(), qName.name(),
                    ExtendedSystemPropertyKeys.prefix.name(), qName.namespace()
            ).stream().filter(n -> n.hasLabel(ExtendedSystemLabels.Function)).forEach(node -> {
                UserFunctionDescriptor descriptor = userFunctionDescriptor(node);
                registerFunction(descriptor.getSignature());
                node.delete();
                setLastUpdate(tx);
            });
            return null;
        });

    }

    public abstract class ProcedureOrFunctionDescriptor {
        private final String statement;

        protected ProcedureOrFunctionDescriptor(String statement) {
            this.statement = statement;
        }

        public String getStatement() {
            return statement;
        }

        abstract public void register();
    }

    public class ProcedureDescriptor extends ProcedureOrFunctionDescriptor {
        private final ProcedureSignature signature;

        public ProcedureDescriptor(ProcedureSignature signature, String statement) {
            super(statement);
            this.signature = signature;
        }

        public ProcedureSignature getSignature() {
            return signature;
        }

        @Override
        public void register() {
            registerProcedure(getSignature(), getStatement());
        }
    }

    public class UserFunctionDescriptor extends ProcedureOrFunctionDescriptor {
        private final UserFunctionSignature signature;
        private final boolean forceSingle;
        private final boolean mapResult;

        public UserFunctionDescriptor(UserFunctionSignature signature, String statement, boolean forceSingle, boolean mapResult) {
            super(statement);
            this.signature = signature;
            this.forceSingle = forceSingle;
            this.mapResult = mapResult;
        }

        public UserFunctionSignature getSignature() {
            return signature;
        }

        public boolean isForceSingle() {
            return forceSingle;
        }

        @Override
        public void register() {
            registerFunction(getSignature(), getStatement(), isForceSingle(), mapResult);
        }
    }
}