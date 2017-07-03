package apoc.schema;

import apoc.result.AssertSchemaResult;
import apoc.result.BooleanResult;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.neo4j.graphdb.Label.label;

public class Schemas {
    @Context
    public GraphDatabaseAPI db;

    @Procedure(value = "apoc.schema.assert", mode = Mode.SCHEMA)
    @Description("apoc.schema.assert({indexLabel:[indexKeys], ...}, {constraintLabel:[constraintKeys], ...}, dropExisting : true) yield label, key, unique, action - drops all other existing indexes and constraints when `dropExisting` is `true` (default is `true`), and asserts that at the end of the operation the given indexes and unique constraints are there, each label:key pair is considered one constraint/label")
    public Stream<AssertSchemaResult> schemaAssert(@Name("indexes") Map<String, List<String>> indexes, @Name("constraints") Map<String, List<String>> constraints, @Name(value = "dropExisting", defaultValue = "true") boolean dropExisting) throws ExecutionException, InterruptedException {
        return Stream.concat(
                assertIndexes(indexes, dropExisting).stream(),
                assertConstraints(constraints, dropExisting).stream());
    }

    /**
     * This procedure checks only if the index exists for a specific Label and properties
     *
     * @param indexLabel
     * @param properties
     * @return true if there is an index for indexLabel and propertyName
     */
    @Procedure(value = "apoc.schema.existsIndex", mode = Mode.SCHEMA)
    @Description("apoc.schema.existsIndex(indexLabel, propertyName) yield value")
    public Stream<BooleanResult> existsIndex(@Name("indexLabel") String indexLabel, @Name("properties") List<String> properties) {
        return Stream.of(new BooleanResult(existsIndexByLabelAndProperties(indexLabel, properties)));
    }

    @Procedure(value = "apoc.schema.existsConstraintForNode", mode = Mode.SCHEMA)
    @Description("apoc.schema.existsConstraintForNode(indexLabel, propertyName, constraintType) yield value - you can specify two different type of constraints: NODE_PROPERTY_EXISTENCE|UNIQUENESS")
    public Stream<BooleanResult> existsConstraintForNode(@Name("indexLabel") String indexLabel, @Name("propertyName") String propertyName, @Name("constraintType") String constraintType) {
        ConstraintType type;
        try {
            type = ConstraintType.valueOf(constraintType);
        } catch (Exception e) {
            throw new RuntimeException("Constraint type must be one of the following values: " + ConstraintType.UNIQUENESS + ", " + ConstraintType.NODE_PROPERTY_EXISTENCE);
        }


        if (ConstraintType.UNIQUENESS.equals(type) || ConstraintType.NODE_PROPERTY_EXISTENCE.equals(type)) {
            return Stream.of(new BooleanResult(existsConstraintOnNode(indexLabel, propertyName, type)));
        }
        throw new RuntimeException("Constraint type must be one of the following values: " + ConstraintType.UNIQUENESS + ", " + ConstraintType.NODE_PROPERTY_EXISTENCE);
    }

    @Procedure(value = "apoc.schema.existsConstraintForRelationship", mode = Mode.SCHEMA)
    @Description("apoc.schema.existsConstraintForRelationship(relationshipType, propertyName) yield value")
    public Stream<BooleanResult> existsConstraintForRelationship(@Name("relationshipType") String relationshipType, @Name("propertyName") String propertyName) {
        return Stream.of(new BooleanResult(existsConstraintOnRelationship(relationshipType, propertyName)));
    }

    public List<AssertSchemaResult> assertConstraints(Map<String, List<String>> constraints0, boolean dropExisting) throws ExecutionException, InterruptedException {
        Map<String, List<String>> constraints = copy(constraints0);
        List<AssertSchemaResult> result = new ArrayList<>(constraints.size());
        Schema schema = db.schema();

        for (ConstraintDefinition definition : schema.getConstraints()) {
            if (!definition.isConstraintType(ConstraintType.UNIQUENESS)) continue;

            String label = definition.getLabel().name();
            String key = Iterables.single(definition.getPropertyKeys());

            AssertSchemaResult info = new AssertSchemaResult(label, key).unique();
            if (!constraints.containsKey(label) || !constraints.get(label).remove(key)) {
                if (dropExisting) {
                    definition.drop();
                    info.dropped();
                }
            }
            result.add(info);
        }

        for (Map.Entry<String, List<String>> constraint : constraints.entrySet()) {
            for (String key : constraint.getValue()) {
                schema.constraintFor(label(constraint.getKey())).assertPropertyIsUnique(key).create();
                result.add(new AssertSchemaResult(constraint.getKey(), key).unique().created());
            }
        }

        return result;
    }

    public List<AssertSchemaResult> assertIndexes(Map<String, List<String>> indexes0, boolean dropExisting) throws ExecutionException, InterruptedException {
        Schema schema = db.schema();
        Map<String, List<String>> indexes = copy(indexes0);
        List<AssertSchemaResult> result = new ArrayList<>(indexes.size());

        for (IndexDefinition definition : schema.getIndexes()) {
            if (definition.isConstraintIndex()) continue;
            String label = definition.getLabel().name();
            String key = Iterables.single(definition.getPropertyKeys());
            AssertSchemaResult info = new AssertSchemaResult(label, key);

            if (!indexes.containsKey(label) || !indexes.get(label).remove(key)) {
                if (dropExisting) {
                    definition.drop();
                    info.dropped();
                }
            }

            result.add(info);
        }

        for (Map.Entry<String, List<String>> index : indexes.entrySet()) {
            for (String key : index.getValue()) {
                schema.indexFor(label(index.getKey())).on(key).create();
                result.add(new AssertSchemaResult(index.getKey(), key).created());
            }
        }

        return result;
    }

    private Map<String, List<String>> copy(Map<String, List<String>> input) {
        if (input == null) {
            return Collections.emptyMap();
        }

        HashMap<String, List<String>> result = new HashMap<>(input.size());
        if (input == null) return result;
        input.forEach((k, v) -> result.put(k, new ArrayList<>(v)));
        return result;
    }

    private Boolean existsIndexByLabelAndProperties(String label, List<String> properties) {
        Schema schema = db.schema();
        for (IndexDefinition indexDefinition : schema.getIndexes()) {
            String labelName = indexDefinition.getLabel().name();
            Iterable<String> keys = indexDefinition.getPropertyKeys();

            if (labelName.equals(label) && Iterables.asCollection(keys).containsAll(properties)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Checks a constraint (by uniqueness or existence) on a Node
     *
     * @param label
     * @param propertyName
     * @param constraintType
     * @return
     */
    private Boolean existsConstraintOnNode(String label, String propertyName, ConstraintType constraintType) {
        Schema schema = db.schema();
        for (ConstraintDefinition definition : schema.getConstraints()) {
            if (definition.getConstraintType().equals(constraintType)) {
                String labelName = definition.getLabel().name();
                String key = Iterables.single(definition.getPropertyKeys());

                if (labelName.equals(label) && key.equals(propertyName)) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Checks a constraint by existence on a Relationship
     *
     * @param type
     * @param propertyName
     * @return
     */
    private Boolean existsConstraintOnRelationship(String type, String propertyName) {
        Schema schema = db.schema();
        for (ConstraintDefinition definition : schema.getConstraints()) {
            if (definition.getConstraintType().equals(ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE)) {
                String relType = definition.getRelationshipType().name();
                String key = Iterables.single(definition.getPropertyKeys());

                if (relType.equals(type) && key.equals(propertyName)) {
                    return true;
                }
            }
        }

        return false;
    }
}