package apoc.schema;

import apoc.result.AssertSchemaResult;
import apoc.result.ConstraintRelationshipInfo;
import apoc.result.IndexConstraintNodeInfo;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.neo4j.graphdb.Label.label;

public class Schemas {
    @Context
    public GraphDatabaseAPI db;

    @Context
    public KernelTransaction tx;

    @Procedure(value = "apoc.schema.assert", mode = Mode.SCHEMA)
    @Description("apoc.schema.assert({indexLabel:[indexKeys], ...}, {constraintLabel:[constraintKeys], ...}, dropExisting : true) yield label, key, unique, action - drops all other existing indexes and constraints when `dropExisting` is `true` (default is `true`), and asserts that at the end of the operation the given indexes and unique constraints are there, each label:key pair is considered one constraint/label")
    public Stream<AssertSchemaResult> schemaAssert(@Name("indexes") Map<String, List<String>> indexes, @Name("constraints") Map<String, List<String>> constraints, @Name(value = "dropExisting", defaultValue = "true") boolean dropExisting) throws ExecutionException, InterruptedException {
        return Stream.concat(
                assertIndexes(indexes, dropExisting).stream(),
                assertConstraints(constraints, dropExisting).stream());
    }

    @Procedure(value = "apoc.schema.nodes", mode = Mode.SCHEMA)
    @Description("CALL apoc.schema.nodes() yield name, label, properties, status, type")
    public Stream<IndexConstraintNodeInfo> nodes() {
        return indexesAndConstraintsForNode();
    }

    @Procedure(value = "apoc.schema.relationships", mode = Mode.SCHEMA)
    @Description("CALL apoc.schema.relationships() yield name, startLabel, type, endLabel, properties, status")
    public Stream<ConstraintRelationshipInfo> relationships() {
        return constraintsForRelationship();
    }

    @UserFunction(value = "apoc.schema.node.indexExists")
    @Description("RETURN apoc.schema.node.indexExists(labelName, propertyNames)")
    public Boolean indexExistsOnNode(@Name("labelName") String labelName, @Name("propertyName") List<String> propertyNames) {
        return indexExists(labelName, propertyNames);
    }

    @UserFunction(value = "apoc.schema.node.constraintExists")
    @Description("RETURN apoc.schema.node.constraintExists(labelName, propertyNames)")
    public Boolean constraintExistsOnNode(@Name("labelName") String labelName, @Name("propertyName") List<String> propertyNames) {
        return constraintsExists(labelName, propertyNames);
    }

    @UserFunction(value = "apoc.schema.relationship.constraintExists")
    @Description("RETURN apoc.schema.relationship.constraintExists(type, propertyNames)")
    public Boolean constraintExistsOnRelationship(@Name("type") String type, @Name("propertyName") List<String> propertyNames) {
        return constraintsExistsForRelationship(type, propertyNames);
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

            Boolean alreadyDropped = false;
            for (String key : definition.getPropertyKeys()) {

                AssertSchemaResult info = new AssertSchemaResult(label, key);

                if (!indexes.containsKey(label) || !indexes.get(label).remove(key)) {
                    if (dropExisting && !alreadyDropped) {
                        definition.drop();
                        alreadyDropped = true;
                    }
                    if(alreadyDropped)
                        info.dropped();
                }
                result.add(info);
            }
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

    /**
     * Checks if an index exists for a given label and a list of properties
     * This method checks for index on nodes
     *
     * @param labelName
     * @param propertyNames
     * @return true if the index exists otherwise it returns false
     */
    private Boolean indexExists(String labelName, List<String> propertyNames) {
        Schema schema = db.schema();

        for (IndexDefinition indexDefinition : Iterables.asList(schema.getIndexes(Label.label(labelName)))) {
            List<String> properties = Iterables.asList(indexDefinition.getPropertyKeys());

            if (properties.equals(propertyNames)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Checks if a constraint exists for a given label and a list of properties
     * This method checks for constraints on node
     *
     * @param labelName
     * @param propertyNames
     * @return true if the constraint exists otherwise it returns false
     */
    private Boolean constraintsExists(String labelName, List<String> propertyNames) {
        Schema schema = db.schema();

        for (ConstraintDefinition constraintDefinition : Iterables.asList(schema.getConstraints(Label.label(labelName)))) {
            List<String> properties = Iterables.asList(constraintDefinition.getPropertyKeys());

            if (properties.equals(propertyNames)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Checks if a constraint exists for a given type and a list of properties
     * This method checks for constraints on relationships
     *
     * @param type
     * @param propertyNames
     * @return true if the constraint exists otherwise it returns false
     */
    private Boolean constraintsExistsForRelationship(String type, List<String> propertyNames) {
        Schema schema = db.schema();

        for (ConstraintDefinition constraintDefinition : Iterables.asList(schema.getConstraints(RelationshipType.withName(type)))) {
            List<String> properties = Iterables.asList(constraintDefinition.getPropertyKeys());

            if (properties.equals(propertyNames)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Collects indexes and constraints for nodes
     *
     * @return
     */
    private Stream<IndexConstraintNodeInfo> indexesAndConstraintsForNode() {
        Schema schema = db.schema();

        // Indexes
        Stream<IndexConstraintNodeInfo> indexes = StreamSupport.stream(schema.getIndexes().spliterator(), false)
                .filter(indexDefinition -> !indexDefinition.isConstraintIndex())
                .map(indexDefinition -> this.nodeInfoFromIndexDefinition(indexDefinition, schema));

        // Constraints
        Stream<IndexConstraintNodeInfo> constraints = StreamSupport.stream(schema.getConstraints().spliterator(), false)
                .filter(constraintDefinition -> !constraintDefinition.isConstraintType(ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE))
                .map(this::nodeInfoFromConstraintDefinition);

        return Stream.concat(indexes, constraints);
    }

    /**
     * Collects constraints for relationships
     *
     * @return
     */
    private Stream<ConstraintRelationshipInfo> constraintsForRelationship() {
        Schema schema = db.schema();

        return StreamSupport.stream(schema.getConstraints().spliterator(), false)
                .filter(constraintDefinition -> constraintDefinition.isConstraintType(ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE))
                .map(this::relationshipInfoFromConstraintDefinition);
    }

    /**
     * Index info from IndexDefinition
     *
     * @param indexDefinition
     * @param schema
     * @return
     */
    private IndexConstraintNodeInfo nodeInfoFromIndexDefinition(IndexDefinition indexDefinition, Schema schema) {
        String labelName = indexDefinition.getLabel().name();
        List<String> properties = Iterables.asList(indexDefinition.getPropertyKeys());

        return new IndexConstraintNodeInfo(
                // Pretty print for index name
                String.format(":%s(%s)", labelName, StringUtils.join(properties, ",")),
                labelName,
                properties,
                schema.getIndexState(indexDefinition).name(),
                "INDEX");
    }

    /**
     * Constraint info from ConstraintDefinition for nodes
     *
     * @param constraintDefinition
     * @return
     */
    private IndexConstraintNodeInfo nodeInfoFromConstraintDefinition(ConstraintDefinition constraintDefinition) {
        return new IndexConstraintNodeInfo(
                String.format("CONSTRAINT %s", constraintDefinition.toString()),
                constraintDefinition.getLabel().name(),
                Iterables.asList(constraintDefinition.getPropertyKeys()),
                "",
                constraintDefinition.getConstraintType().name()
        );
    }

    /**
     * Constraint info from ConstraintDefinition for relationships
     *
     * @param constraintDefinition
     * @return
     */
    private ConstraintRelationshipInfo relationshipInfoFromConstraintDefinition(ConstraintDefinition constraintDefinition) {
        return new ConstraintRelationshipInfo(
                String.format("CONSTRAINT %s", constraintDefinition.toString()),
                constraintDefinition.getConstraintType().name(),
                Iterables.asList(constraintDefinition.getPropertyKeys()),
                ""
        );
    }
}