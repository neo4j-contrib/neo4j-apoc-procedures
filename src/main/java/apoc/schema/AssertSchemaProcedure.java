package apoc.schema;

import org.neo4j.collection.RawIterator;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.*;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Name;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;
import static org.neo4j.helpers.collection.Iterables.asList;
import static org.neo4j.kernel.api.proc.Neo4jTypes.*;

/**
 * @author mh
 * @since 12.05.16
 */
public class AssertSchemaProcedure implements CallableProcedure {

    public static class SchemaInfo {
        public final String label;
        public final List<String> keys;
        public boolean unique = false;
        public String action = "KEPT";

        public SchemaInfo(Map<String, List<String>> info) {
            Map.Entry<String, List<String>> entry = info.entrySet().iterator().next();
            this.label = entry.getKey();
            this.keys = entry.getValue();
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
    public Stream<SchemaInfo> assertSchema(@Name("indexes") List<Map<String, List<String>>> indexes, @Name("constraints") List<Map<String, List<String>>> constraints) throws ExecutionException, InterruptedException {
        return Stream.concat(
                assertIndexes(indexes).stream(),
                assertConstraints(constraints).stream());
    }

    public List<SchemaInfo> assertConstraints(List<Map<String, List<String>>> constraints0) throws ExecutionException, InterruptedException {
        List<Map<String, List<String>>> constraints = new ArrayList<>(constraints0 == null ? Collections.emptyList() : constraints0);
        List<SchemaInfo> result = new ArrayList<>(constraints.size());
        Schema schema = db.schema();
        for (ConstraintDefinition definition : schema.getConstraints()) {
            if (!definition.isConstraintType(ConstraintType.UNIQUENESS)) continue;

            Map<String, List<String>> constraint = singletonMap(definition.getLabel().name(), asList(definition.getPropertyKeys()));

            SchemaInfo info = new SchemaInfo(constraint).unique();
            if (!constraints.remove(constraint)) {
                definition.drop();
                info.dropped();
            }
            result.add(info);
        }
        for (Map<String, List<String>> constraint : constraints) {
            for (Map.Entry<String, List<String>> entry : constraint.entrySet()) {
                createConstraint(schema, entry.getKey(), entry.getValue());
                result.add(new SchemaInfo(constraint).unique().created());
            }
        }
        return result;
    }

    public List<SchemaInfo> assertIndexes(List<Map<String, List<String>>> indexes0) throws ExecutionException, InterruptedException {
        Schema schema = db.schema();
        List<Map<String, List<String>>> indexes = new ArrayList<>(indexes0 == null ? Collections.emptyList() : indexes0);
        List<SchemaInfo> result = new ArrayList<>(indexes.size());
        for (IndexDefinition definition : schema.getIndexes()) {
            if (definition.isConstraintIndex()) continue;
            Map<String, List<String>> index = singletonMap(definition.getLabel().name(), asList(definition.getPropertyKeys()));
            SchemaInfo info = new SchemaInfo(index);
            if (!indexes.remove(index)) {
                definition.drop();
                info.dropped();
            }
            result.add(info);
        }
        for (Map<String, List<String>> index : indexes) {
            for (Map.Entry<String, List<String>> entry : index.entrySet()) {
                createIndex(schema, entry.getKey(), entry.getValue());
                result.add(new SchemaInfo(index).created());
            }
        }
        return result;
    }

    public void createIndex(Schema schema, String label, List<String> keys) {
        IndexCreator indexCreator = schema.indexFor(Label.label(label));
        for (String prop : keys) {
            indexCreator = indexCreator.on(prop);
        }
        indexCreator.create();
    }

    public void createConstraint(Schema schema, String label, List<String> keys) {
        ConstraintCreator constraintCreator = schema.constraintFor(Label.label(label));
        for (String prop : keys) {
            constraintCreator = constraintCreator.assertPropertyIsUnique(prop);
        }
        constraintCreator.create();
    }

    @Override
    public RawIterator<Object[], ProcedureException> apply(final Context ctx, final Object[] input) throws ProcedureException {
        List<Map<String, List<String>>> indexes = (List<Map<String, List<String>>>) input[1];
        List<Map<String, List<String>>> constraints = (List<Map<String, List<String>>>) input[0];
        try {
            Iterator<SchemaInfo> it = assertSchema(constraints, indexes).iterator();
            return RawIterator.from((ThrowingSupplier<Object[], ProcedureException>) () -> {
                if (it.hasNext()) {
                    SchemaInfo info = it.next();
                    return new Object[]{info.label, info.keys, info.unique, info.action};
                }
                return null;
            });
        } catch (InterruptedException | ExecutionException e) {
            throw new ProcedureException(Status.General.UnknownError, e, e.getMessage());
        }
    }

    @Override
    public ProcedureSignature signature() {
        return ProcedureSignature.procedureSignature("apoc","schema","assert")
                .mode(ProcedureSignature.Mode.DBMS)
                .in("indexes", NTList(NTMap))
                .in("constraints", NTList(NTMap))
                .out("label", NTString)
                .out("keys", NTList(NTString))
                .out("unique", NTBoolean)
                .out("action", NTString)
                .build();
    }
}
