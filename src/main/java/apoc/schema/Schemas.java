package apoc.schema;

import apoc.Description;
import apoc.Pools;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;
import static org.neo4j.helpers.collection.Iterables.asList;

/**
 * @author mh
 * @since 12.05.16
 */
public class Schemas {

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

    @Context
    public GraphDatabaseAPI db;
    @Context
    public Log log;

    @Procedure("apoc.schema.assert")
    @PerformsWrites
    @Description("apoc.schema.assert([{label:[key1,key2]},..],[{label:[key1,key2]},..]) yield label, keys, unique, action - asserts that at the end of the operation the given indexes and unique constraints are there")
    public Stream<SchemaInfo> assertSchema(@Name("indexes") List<Map<String, List<String>>> indexes, @Name("constraints") List<Map<String, List<String>>> constraints) throws ExecutionException, InterruptedException {
        return Stream.concat(
                assertIndexes(indexes).stream(),
                assertConstraints(constraints).stream());
    }

    public List<SchemaInfo> assertConstraints(List<Map<String, List<String>>> constraints0) throws ExecutionException, InterruptedException {
        List<Map<String, List<String>>> constraints = new ArrayList<>(constraints0 == null ? Collections.emptyList() : constraints0);
        List<SchemaInfo> result = new ArrayList<>(constraints.size());
        Schema schema = db.schema();
        intTx(db, () -> {
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
            return result;
        });
        intTx(db, () -> {
            for (Map<String, List<String>> constraint : constraints) {
                for (Map.Entry<String, List<String>> entry : constraint.entrySet()) {
                    createConstraint(schema, entry.getKey(), entry.getValue());
                    result.add(new SchemaInfo(constraint).unique().created());
                }
            }
            return result;
        });
        return result;
    }

    public List<SchemaInfo> assertIndexes(List<Map<String, List<String>>> indexes0) throws ExecutionException, InterruptedException {
        Schema schema = db.schema();
        List<Map<String, List<String>>> indexes = new ArrayList<>(indexes0 == null ? Collections.emptyList() : indexes0);
        List<SchemaInfo> result = new ArrayList<>(indexes.size());
        intTx(db, () -> {
            for (IndexDefinition definition : schema.getIndexes()) {
                Map<String, List<String>> index = singletonMap(definition.getLabel().name(), asList(definition.getPropertyKeys()));
                SchemaInfo info = new SchemaInfo(index);
                if (!indexes.remove(index)) {
                    definition.drop();
                    info.dropped();
                }
                result.add(info);
            }
            return result;
        });
        intTx(db, () -> {
            for (Map<String, List<String>> index : indexes) {
                for (Map.Entry<String, List<String>> entry : index.entrySet()) {
                    createIndex(schema, entry.getKey(), entry.getValue());
                    result.add(new SchemaInfo(index).created());
                }
            }
            return result;
        });
        return result;
    }

    public static <T> T intTx(GraphDatabaseAPI db, Callable<T> callable) {
        try {
            return Pools.SINGLE.submit(() -> {
                try (Transaction tx = db.beginTx()) {
                    T result = callable.call();
                    tx.success();
                    return result;
                }
            }).get();
        } catch (Exception e) {
            throw new RuntimeException("Error executing in separate transaction", e);
        }
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
}
