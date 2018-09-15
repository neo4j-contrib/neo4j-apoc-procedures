package apoc.custom;

import apoc.util.JsonUtil;
import org.neo4j.collection.PrefetchingRawIterator;
import org.neo4j.collection.RawIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.*;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.CallableUserFunction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.AnyValue;
import org.neo4j.values.VirtualValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.*;

/**
 * @author mh
 * @since 18.08.18
 */
public class CypherProcedures {
    private static final String PREFIX = "custom";
    @Context
    public GraphDatabaseAPI api;

    /*
    * store in graph properties, load at startup
    * allow to register proper params as procedure-params
    * allow to register proper return columns
    * allow to register mode
     */
    @Procedure(value = "apoc.custom.asProcedure",mode = Mode.WRITE)
    public void asProcedure(@Name("name") String name, @Name("statement") String statement,
                         @Name(value = "mode",defaultValue = "read") String mode,
                         @Name(value= "outputs", defaultValue = "null") List<List<String>> outputs,
                         @Name(value= "inputs", defaultValue = "null") List<List<String>> inputs
                         ) throws ProcedureException {
        Procedures procedures = api.getDependencyResolver().resolveDependency(Procedures.class);
        ProcedureSignature signature = new ProcedureSignature(qualifiedName(name), inputSignatures(inputs), outputSignatures(outputs),
                Mode.valueOf(mode.toUpperCase()), null, new String[0], null, null, false);

        procedures.register(new CallableProcedure.BasicProcedure(signature) {
            @Override
            public RawIterator<Object[], ProcedureException> apply(org.neo4j.kernel.api.proc.Context ctx, Object[] input, ResourceTracker resourceTracker) throws ProcedureException {
                Map<String, Object> params = params(input, inputs);
                Result result = api.execute(statement, params);
                resourceTracker.registerCloseableResource(result);
                String[] names = outputs == null ? null : outputs.stream().map(pair -> pair.get(0)).toArray(String[]::new);
                return new PrefetchingRawIterator<Object[], ProcedureException>() {
                    @Override
                    protected Object[] fetchNextOrNull() throws ProcedureException {
                        if (!result.hasNext()) return null;
                        Map<String, Object> row = result.next();
                        return toResult(row, names);
                    }
                };
            }
        }, true);
    }
    @Procedure(value = "apoc.custom.asFunction",mode = Mode.WRITE)
    public void asFunction(@Name("name") String name, @Name("statement") String statement,
                         @Name(value= "outputs", defaultValue = "") String outputs,
                         @Name(value= "inputs", defaultValue = "null") List<List<String>> inputs,
                        @Name(value = "forceSingle",defaultValue = "false") boolean forceSingle) throws ProcedureException {
        Procedures procedures = api.getDependencyResolver().resolveDependency(Procedures.class);

        AnyType outType = typeof(outputs.isEmpty() ? "LIST OF MAP" : outputs);
        UserFunctionSignature signature = new UserFunctionSignature(qualifiedName(name), inputSignatures(inputs), outType,
                null, new String[0], null, false);

        DefaultValueMapper defaultValueMapper = new DefaultValueMapper(api.getDependencyResolver().resolveDependency(GraphDatabaseFacade.class));

        procedures.register(new CallableUserFunction.BasicUserFunction(signature) {
            @Override
            public AnyValue apply(org.neo4j.kernel.api.proc.Context ctx, AnyValue[] input) throws ProcedureException {
                Map<String, Object> params = functionParams(input, inputs,defaultValueMapper);
                try (Result result = api.execute(statement, params)) {
//                resourceTracker.registerCloseableResource(result); // TODO
                    if (!result.hasNext()) return null;
                    if (outputs.isEmpty()) {
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
                    throw new IllegalStateException("Result mismatch " + cols + " output type is " + outputs);
                }
            }
        }, true);
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

    public QualifiedName qualifiedName(@Name("name") String name) {
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
}
