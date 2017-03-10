package apoc.schema;

import org.neo4j.collection.RawIterator;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Mode;


import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.neo4j.graphdb.Label.label;
import static org.neo4j.kernel.api.proc.Neo4jTypes.*;

/**
 * @author mh
 * @since 12.05.16
 */
public class AssertSchemaProcedure implements CallableProcedure {

    public static class SchemaInfo {
        public final String label;
        public final String key;
        public boolean unique = false;
        public String action = "KEPT";

        public SchemaInfo(String label, String key) {
            this.label = label;
            this.key = key;
        }

        public SchemaInfo unique() {
            this.unique = true;
            return this;
        }

        public SchemaInfo dropped() {
            this.action = "DROPPED";
            return this;
        }

        public SchemaInfo created() {
            this.action = "CREATED";
            return this;
        }
    }

    public GraphDatabaseAPI db;
    public Log log;

    public AssertSchemaProcedure(final GraphDatabaseAPI api, final Log log) {
        this.db = api;
        this.log = log;
    }

    //    @Procedure("apoc.schema.assert")
//    @Description("apoc.schema.assert([{label:[key1,key2]},..],[{label:[key1,key2]},..]) yield label, keys, unique, action - asserts that at the end of the operation the given indexes and unique constraints are there")
    public Stream<SchemaInfo> assertSchema(@Name("indexes") Map<String, List<String>> indexes, @Name("constraints") Map<String, List<String>> constraints) throws ExecutionException, InterruptedException {
        return Stream.concat(
                assertIndexes(indexes).stream(),
                assertConstraints(constraints).stream());
    }

    public List<SchemaInfo> assertConstraints(Map<String, List<String>> constraints0) throws ExecutionException, InterruptedException {
        Map<String, List<String>> constraints = copy(constraints0);
        List<SchemaInfo> result = new ArrayList<>(constraints.size());
        Schema schema = db.schema();
        for (ConstraintDefinition definition : schema.getConstraints()) {
            if (!definition.isConstraintType(ConstraintType.UNIQUENESS)) continue;

            String label = definition.getLabel().name();
            String key = Iterables.single(definition.getPropertyKeys());
            SchemaInfo info = new SchemaInfo(label, key).unique();
            if (!constraints.containsKey(label) || !constraints.get(label).remove(key)) {
                definition.drop();
                info.dropped();
            }
            result.add(info);
        }
        for (Map.Entry<String, List<String>> constraint : constraints.entrySet()) {
            for (String key : constraint.getValue()) {
                schema.constraintFor(label(constraint.getKey())).assertPropertyIsUnique(key).create();
                result.add(new SchemaInfo(constraint.getKey(), key).unique().created());
            }
        }
        return result;
    }

    private Map<String, List<String>> copy(Map<String, List<String>> input) {
        HashMap<String, List<String>> result = new HashMap<>();
        if (input==null) return result;
        input.forEach((k,v) -> result.put(k,new ArrayList<>(v)));
        return result;
    }

    public List<SchemaInfo> assertIndexes(Map<String, List<String>> indexes0) throws ExecutionException, InterruptedException {
        Schema schema = db.schema();
        Map<String, List<String>> indexes = copy(indexes0);
        List<SchemaInfo> result = new ArrayList<>(indexes.size());
        for (IndexDefinition definition : schema.getIndexes()) {
            if (definition.isConstraintIndex()) continue;
            String label = definition.getLabel().name();
            String key = Iterables.single(definition.getPropertyKeys());
            SchemaInfo info = new SchemaInfo(label, key);
            if (!indexes.containsKey(label) || !indexes.get(label).remove(key)) {
                definition.drop();
                info.dropped();
            }
            result.add(info);
        }
        for (Map.Entry<String, List<String>> index : indexes.entrySet()) {
            for (String key : index.getValue()) {
                schema.indexFor(label(index.getKey())).on(key).create();
                result.add(new SchemaInfo(index.getKey(), key).created());
            }
        }
        return result;
    }

    @Override
    public RawIterator<Object[], ProcedureException> apply(Context ctx, Object[] input) throws ProcedureException {
        Map<String, List<String>> indexes = (Map<String, List<String>>) input[1];
        Map<String, List<String>> constraints = (Map<String, List<String>>) input[0];
        try {
            Iterator<SchemaInfo> it = assertSchema(constraints, indexes).iterator();
            return RawIterator.from((ThrowingSupplier<Object[], ProcedureException>) () -> {
                if (it.hasNext()) {
                    SchemaInfo info = it.next();
                    return new Object[]{info.label, info.key, info.unique, info.action};
                }
                return null;
            });
        } catch (InterruptedException | ExecutionException e) {
            throw new ProcedureException(Status.General.UnknownError, e, e.getMessage());
        }
    }

    @Override
    public ProcedureSignature signature() {
        return ProcedureSignature.procedureSignature("apoc", "schema", "assert")
                .mode(Mode.DBMS)
                .in("indexes", NTMap)
                .in("constraints", NTMap)
                .out("label", NTString)
                .out("key", NTList(NTString))
                .out("unique", NTBoolean)
                .out("action", NTString)
                .build();
    }
}
