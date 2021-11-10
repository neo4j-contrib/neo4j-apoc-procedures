package apoc.schema;

import apoc.Pools;
import apoc.result.AssertSchemaResult;
import apoc.result.IndexConstraintNodeInfo;
import apoc.result.IndexConstraintRelationshipInfo;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;
import org.neo4j.token.api.TokenConstants;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.schema.SchemaUserDescription.TOKEN_LABEL;
import static org.neo4j.internal.schema.SchemaUserDescription.TOKEN_REL_TYPE;

public class Schemas {
    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Context
    public KernelTransaction ktx;

    @Procedure(value = "apoc.schema.assert", mode = Mode.SCHEMA)
    @Description("apoc.schema.assert({indexLabel:[[indexKeys]], ...}, {constraintLabel:[constraintKeys], ...}, dropExisting : true) yield label, key, keys, unique, action - drops all other existing indexes and constraints when `dropExisting` is `true` (default is `true`), and asserts that at the end of the operation the given indexes and unique constraints are there, each label:key pair is considered one constraint/label. Non-constraint indexes can define compound indexes with label:[key1,key2...] pairings.")
    public Stream<AssertSchemaResult> schemaAssert(@Name("indexes") Map<String, List<Object>> indexes, @Name("constraints") Map<String, List<Object>> constraints, @Name(value = "dropExisting", defaultValue = "true") boolean dropExisting) throws ExecutionException, InterruptedException {
        return Stream.concat(
                assertIndexes(indexes, dropExisting).stream(),
                assertConstraints(constraints, dropExisting).stream());
    }

    @Procedure(value = "apoc.schema.nodes", mode = Mode.SCHEMA)
    @Description("CALL apoc.schema.nodes([config]) yield name, label, properties, status, type")
    public Stream<IndexConstraintNodeInfo> nodes(@Name(value = "config",defaultValue = "{}") Map<String,Object> config) throws IndexNotFoundKernelException {
        return indexesAndConstraintsForNode(config);
    }

    @Procedure(value = "apoc.schema.relationships", mode = Mode.SCHEMA)
    @Description("CALL apoc.schema.relationships([config]) yield name, startLabel, type, endLabel, properties, status")
    public Stream<IndexConstraintRelationshipInfo> relationships(@Name(value = "config",defaultValue = "{}") Map<String,Object> config) {
        return indexesAndConstraintsForRelationships(config);
    }

    @UserFunction(value = "apoc.schema.node.indexExists")
    @Description("RETURN apoc.schema.node.indexExists(labelName, propertyNames)")
    public Boolean indexExistsOnNode(@Name("labelName") String labelName, @Name("propertyName") List<String> propertyNames) {
        return indexExists(labelName, propertyNames);
    }

    @UserFunction(value = "apoc.schema.relationship.indexExists")
    @Description("RETURN apoc.schema.relationship.indexExists(relName, propertyNames)")
    public Boolean indexExistsOnRelationship(@Name("labelName") String relName, @Name("propertyName") List<String> propertyNames) {
        return indexExistsForRelationship(relName, propertyNames);
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

    public List<AssertSchemaResult> assertConstraints(Map<String, List<Object>> constraints0, boolean dropExisting) throws ExecutionException, InterruptedException {
        Map<String, List<Object>> constraints = copyMapOfObjects(constraints0);
        List<AssertSchemaResult> result = new ArrayList<>(constraints.size());
        Schema schema = tx.schema();

        for (ConstraintDefinition definition : schema.getConstraints()) {
            String label = definition.isConstraintType(ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE) ? definition.getRelationshipType().name() : definition.getLabel().name();
            AssertSchemaResult info = new AssertSchemaResult(label, Iterables.asList(definition.getPropertyKeys())).unique();
            if (!checkIfConstraintExists(label, constraints, info)) {
                if (dropExisting) {
                    definition.drop();
                    info.dropped();
                }
            }
            result.add(info);
        }

        for (Map.Entry<String, List<Object>> constraint : constraints.entrySet()) {
            for (Object key : constraint.getValue()) {
                if (key instanceof String) {
                    result.add(createUniqueConstraint(schema, constraint.getKey(), key.toString()));
                } else if (key instanceof List) {
                    result.add(createNodeKeyConstraint(constraint.getKey(), (List<Object>) key));
                }
            }
        }
        return result;
    }

    private boolean checkIfConstraintExists(String label, Map<String, List<Object>> constraints, AssertSchemaResult info) {
        if (constraints.containsKey(label)) {
            return constraints.get(label).removeIf(item -> {
                // when there is a constraint IS UNIQUE
                if (item instanceof String) {
                    return item.equals(info.key);
                // when there is a constraint IS NODE KEY
                } else {
                    return info.keys.equals(item);
                }
            });
        }
        return false;
    }

    private AssertSchemaResult createNodeKeyConstraint(String lbl, List<Object> keys) {
        String keyProperties = keys.stream()
                .map( property -> String.format("n.`%s`", property))
                .collect( Collectors.joining( "," ) );
        tx.execute(String.format("CREATE CONSTRAINT ON (n:`%s`) ASSERT (%s) IS NODE KEY", lbl, keyProperties)).close();
        List<String> keysToSting = keys.stream().map(Object::toString).collect(Collectors.toList());
        return new AssertSchemaResult(lbl, keysToSting).unique().created();
    }

    private AssertSchemaResult createUniqueConstraint(Schema schema, String lbl, String key) {
        schema.constraintFor(label(lbl)).assertPropertyIsUnique(key).create();
        return new AssertSchemaResult(lbl, key).unique().created();
    }

    public List<AssertSchemaResult> assertIndexes(Map<String, List<Object>> indexes0, boolean dropExisting) throws ExecutionException, InterruptedException, IllegalArgumentException {
        Schema schema = tx.schema();
        Map<String, List<Object>> indexes = copyMapOfObjects(indexes0);
        List<AssertSchemaResult> result = new ArrayList<>(indexes.size());

        for (IndexDefinition definition : schema.getIndexes()) {
            if (!definition.isNodeIndex())
                continue;
            if (definition.getIndexType() == IndexType.LOOKUP)
                continue;
            if (definition.isConstraintIndex())
                continue;

            String label = Iterables.single(definition.getLabels()).name();
            List<String> keys = new ArrayList<>();
            definition.getPropertyKeys().forEach(keys::add);

            AssertSchemaResult info = new AssertSchemaResult(label, keys);
            if(indexes.containsKey(label)) {
                if (keys.size() > 1) {
                    indexes.get(label).remove(keys);
                } else if (keys.size() == 1) {
                    indexes.get(label).remove(keys.get(0));
                } else
                    throw new IllegalArgumentException("Label given with no keys.");
            }

            if (dropExisting) {
                definition.drop();
                info.dropped();
            }

            result.add(info);
        }

        if (dropExisting)
            indexes = copyMapOfObjects(indexes0);

        for (Map.Entry<String, List<Object>> index : indexes.entrySet()) {
            for (Object key : index.getValue()) {
                if (key instanceof String) {
                    result.add(createSinglePropertyIndex(schema, index.getKey(), (String) key));
                } else if (key instanceof List) {
                    result.add(createCompoundIndex(index.getKey(), (List<String>) key));
                }
            }
        }
        return result;
    }

    private AssertSchemaResult createSinglePropertyIndex(Schema schema, String lbl, String key) {
        schema.indexFor(label(lbl)).on(key).create();
        return new AssertSchemaResult(lbl, key).created();
    }

    private AssertSchemaResult createCompoundIndex(String label, List<String> keys) {
        List<String> backTickedKeys = new ArrayList<>();
        keys.forEach(key->backTickedKeys.add(String.format("`%s`", key)));

        tx.execute(String.format("CREATE INDEX ON :`%s` (%s)", label, String.join(",", backTickedKeys))).close();
        return new AssertSchemaResult(label, keys).created();
    }

    private Map<String, List<Object>> copyMapOfObjects(Map<String, List<Object>> input) {
        if (input == null) {
            return Collections.emptyMap();
        }

        HashMap<String, List<Object>> result = new HashMap<>(input.size());

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
        Iterable<IndexDefinition> nodeIndexes = tx.schema().getIndexes(label(labelName));
        return isIndexExistent(propertyNames, nodeIndexes);
    }
    
    private Boolean indexExistsForRelationship(String relName, List<String> propertyNames) {
        Iterable<IndexDefinition> relIndexes = tx.schema().getIndexes(RelationshipType.withName(relName));
        return isIndexExistent(propertyNames, relIndexes);
    }

    private Boolean isIndexExistent(List<String> propertyNames, Iterable<IndexDefinition> indexes) {
        for (IndexDefinition indexDefinition : indexes) {
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
        Schema schema = tx.schema();

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
        Schema schema = tx.schema();

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
    private Stream<IndexConstraintNodeInfo> indexesAndConstraintsForNode(Map<String,Object> config) {

        SchemaConfig schemaConfig = new SchemaConfig(config);
        Set<String> includeLabels = schemaConfig.getLabels();
        Set<String> excludeLabels = schemaConfig.getExcludeLabels();

        try ( Statement ignore = ktx.acquireStatement() ) {
            TokenRead tokenRead = ktx.tokenRead();

            SchemaRead schemaRead = ktx.schemaRead();
            Iterable<IndexDescriptor> indexesIterator;
            Iterable<ConstraintDescriptor> constraintsIterator;

            if (includeLabels.isEmpty()) {

                Iterator<IndexDescriptor> allIndex = schemaRead.indexesGetAll();

                indexesIterator = getIndexesFromSchema(allIndex,
                        index -> index.schema().entityType().equals(EntityType.NODE)
                                && Arrays.stream(index.schema().getEntityTokenIds()).noneMatch(id -> {
                    try {
                        return excludeLabels.contains(tokenRead.nodeLabelName(id));
                    } catch (LabelNotFoundKernelException e) {
                        return false;
                    }
                }));

                Iterable<ConstraintDescriptor> allConstraints = () -> schemaRead.constraintsGetAll();
                constraintsIterator = StreamSupport.stream(allConstraints.spliterator(),false)
                        .filter(constraint -> Arrays.stream(constraint.schema().getEntityTokenIds()).noneMatch(id -> {
                            try {
                                return excludeLabels.contains(tokenRead.nodeLabelName(id));
                            } catch (LabelNotFoundKernelException e) {
                                return false;
                            }
                            }))
                            .collect(Collectors.toList());
            } else {
                constraintsIterator = includeLabels.stream()
                        .filter(label -> !excludeLabels.contains(label) && tokenRead.nodeLabel(label) != -1)
                        .flatMap(label -> {
                            Iterable<ConstraintDescriptor> indexesForLabel = () -> schemaRead.constraintsGetForLabel(tokenRead.nodeLabel(label));
                            return StreamSupport.stream(indexesForLabel.spliterator(), false);
                        })
                        .collect(Collectors.toList());

                indexesIterator = includeLabels.stream()
                        .filter(label -> !excludeLabels.contains(label) && tokenRead.nodeLabel(label) != -1)
                        .flatMap(label -> {
                            Iterable<IndexDescriptor> indexesForLabel = () -> schemaRead.indexesGetForLabel(tokenRead.nodeLabel(label));
                            return StreamSupport.stream(indexesForLabel.spliterator(), false);
                        })
                        .collect(Collectors.toList());
            }


            Stream<IndexConstraintNodeInfo> constraintNodeInfoStream = StreamSupport.stream(constraintsIterator.spliterator(), false)
                    .filter(constraintDescriptor -> constraintDescriptor.type().equals(org.neo4j.internal.schema.ConstraintType.EXISTS))
                    .map(constraintDescriptor -> this.nodeInfoFromConstraintDescriptor(constraintDescriptor, tokenRead))
                    .sorted(Comparator.comparing(i -> i.label));

            Stream<IndexConstraintNodeInfo> indexNodeInfoStream = StreamSupport.stream(indexesIterator.spliterator(), false)
                    .map(indexDescriptor -> this.nodeInfoFromIndexDefinition(indexDescriptor, schemaRead, tokenRead))
                    .sorted(Comparator.comparing(i -> i.label));

            return Stream.of(constraintNodeInfoStream, indexNodeInfoStream).flatMap(e -> e);
        }
    }

    private List<IndexDescriptor> getIndexesFromSchema(Iterator<IndexDescriptor> allIndex, Predicate<IndexDescriptor> indexDescriptorPredicate) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(allIndex, Spliterator.ORDERED), false)
                .filter(indexDescriptorPredicate).collect(Collectors.toList());
    }

    /**
     * Collects constraints for relationships
     *
     * @return
     */
    private Stream<IndexConstraintRelationshipInfo> indexesAndConstraintsForRelationships(Map<String,Object> config) {
        Schema schema = tx.schema();

        SchemaConfig schemaConfig = new SchemaConfig(config);
        Set<String> includeRelationships = schemaConfig.getRelationships();
        Set<String> excludeRelationships = schemaConfig.getExcludeRelationships();

        try ( Statement ignore = ktx.acquireStatement() ) {
            TokenRead tokenRead = ktx.tokenRead();
            SchemaRead schemaRead = ktx.schemaRead();
            Iterable<ConstraintDefinition> constraintsIterator;
            Iterable<IndexDescriptor> indexesIterator;

            if(!includeRelationships.isEmpty()) {
                constraintsIterator = includeRelationships.stream()
                        .filter(type -> !excludeRelationships.contains(type) && tokenRead.relationshipType(type) != TokenConstants.NO_TOKEN)
                        .flatMap(type -> {
                            Iterable<ConstraintDefinition> constraintsForType = schema.getConstraints(RelationshipType.withName(type));
                            return StreamSupport.stream(constraintsForType.spliterator(), false);
                        })
                        .collect(Collectors.toList());

                indexesIterator = includeRelationships.stream()
                        .filter(type -> !excludeRelationships.contains(type) && tokenRead.relationshipType(type) != TokenConstants.NO_TOKEN)
                        .flatMap(type -> {
                            Iterable<IndexDescriptor> indexesForRelType = () -> schemaRead.indexesGetForRelationshipType(tokenRead.relationshipType(type));
                            return StreamSupport.stream(indexesForRelType.spliterator(), false);
                        })
                        .collect(Collectors.toList());
            } else {
                Iterable<ConstraintDefinition> allConstraints = schema.getConstraints();
                constraintsIterator = StreamSupport.stream(allConstraints.spliterator(),false)
                        .filter(index -> !excludeRelationships.contains(index.getRelationshipType().name()))
                        .collect(Collectors.toList());

                Iterator<IndexDescriptor> allIndex = schemaRead.indexesGetAll();
                indexesIterator = getIndexesFromSchema(allIndex, index -> index.schema().entityType().equals(EntityType.RELATIONSHIP)
                        && Arrays.stream(index.schema().getEntityTokenIds())
                        .noneMatch(id -> excludeRelationships.contains(tokenRead.relationshipTypeGetName(id))));
            }

            Stream<IndexConstraintRelationshipInfo> constraintRelationshipInfoStream = StreamSupport.stream(constraintsIterator.spliterator(), false)
                    .filter(constraintDefinition -> constraintDefinition.isConstraintType(ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE))
                    .map(this::relationshipInfoFromConstraintDefinition);

            Stream<IndexConstraintRelationshipInfo> indexRelationshipInfoStream = StreamSupport.stream(indexesIterator.spliterator(), false)
                    .map(index -> relationshipInfoFromIndexDescription(index, tokenRead));

            return Stream.of(constraintRelationshipInfoStream, indexRelationshipInfoStream).flatMap(e -> e);
        }
    }

    /**
     * ConstraintInfo info from ConstraintDescriptor
     *
     * @param constraintDescriptor
     * @param tokens
     * @return
     */
    private IndexConstraintNodeInfo nodeInfoFromConstraintDescriptor(ConstraintDescriptor constraintDescriptor, TokenNameLookup tokens) {
        String labelName =  tokens.labelGetName(constraintDescriptor.schema().getLabelId());
        List<String> properties = new ArrayList<>();
        Arrays.stream(constraintDescriptor.schema().getPropertyIds()).forEach((i) -> properties.add(tokens.propertyKeyGetName(i)));
        return new IndexConstraintNodeInfo(
                // Pretty print for index name
                String.format(":%s(%s)", labelName, StringUtils.join(properties, ",")),
                labelName,
                properties,
                StringUtils.EMPTY,
                ConstraintType.NODE_PROPERTY_EXISTENCE.toString(),
                "NO FAILURE",
                0,
                0,
                0,
                constraintDescriptor.userDescription(tokens)
        );
    }

    /**
     * Index info from IndexDefinition
     *
     * @param indexDescriptor
     * @param schemaRead
     * @param tokens
     * @return
     */
    private IndexConstraintNodeInfo nodeInfoFromIndexDefinition(IndexDescriptor indexDescriptor, SchemaRead schemaRead, TokenNameLookup tokens){
        int[] labelIds = indexDescriptor.schema().getEntityTokenIds();
        int length = labelIds.length;
        if (length > 1) throw new IllegalStateException("Index with more than one label");
        // to handle LOOKUP indexes
        String labelName = length == 0 ? TOKEN_LABEL : tokens.labelGetName(labelIds[0]);
        List<String> properties = new ArrayList<>();
        Arrays.stream(indexDescriptor.schema().getPropertyIds()).forEach((i) -> properties.add(tokens.propertyKeyGetName(i)));
        try {
            return new IndexConstraintNodeInfo(
                    // Pretty print for index name
                    getSchemaInfoName(labelName, properties),
                    labelName,
                    properties,
                    schemaRead.indexGetState(indexDescriptor).toString(),
                    !indexDescriptor.isUnique() ? "INDEX" : "UNIQUENESS",
                    schemaRead.indexGetState(indexDescriptor).equals(InternalIndexState.FAILED) ? schemaRead.indexGetFailure(indexDescriptor) : "NO FAILURE",
                    schemaRead.indexGetPopulationProgress(indexDescriptor).getCompleted() / schemaRead.indexGetPopulationProgress(indexDescriptor).getTotal() * 100,
                    schemaRead.indexSize(indexDescriptor),
                    schemaRead.indexUniqueValuesSelectivity(indexDescriptor),
                    indexDescriptor.userDescription(tokens)
            );
        } catch(IndexNotFoundKernelException e) {
            return new IndexConstraintNodeInfo(
                    // Pretty print for index name
                    getSchemaInfoName(labelName, properties),
                    labelName,
                    properties,
                    "NOT_FOUND",
                    !indexDescriptor.isUnique() ? "INDEX" : "UNIQUENESS",
                    "NOT_FOUND",
                    0,0,0,
                    indexDescriptor.userDescription(tokens)
            );
        }
    }

    private IndexConstraintRelationshipInfo relationshipInfoFromIndexDescription(IndexDescriptor indexDescriptor, TokenNameLookup tokens) {
        int[] relIds = indexDescriptor.schema().getEntityTokenIds();
        int length = relIds.length;
        if (length > 1) throw new IllegalStateException("Index with more than one rel type");
        // to handle LOOKUP indexes
        String relName = length == 0 ? TOKEN_REL_TYPE : tokens.relationshipTypeGetName(relIds[0]);
        final List<String> properties = Arrays.stream(indexDescriptor.schema().getPropertyIds())
                .mapToObj(tokens::propertyKeyGetName)
                .collect(Collectors.toList());
        return new IndexConstraintRelationshipInfo(getSchemaInfoName(relName, properties), relName, properties, "");
    }

    /**
     * Constraint info from ConstraintDefinition for relationships
     *
     * @param constraintDefinition
     * @return
     */
    private IndexConstraintRelationshipInfo relationshipInfoFromConstraintDefinition(ConstraintDefinition constraintDefinition) {
        return new IndexConstraintRelationshipInfo(
                String.format("CONSTRAINT %s", constraintDefinition.toString()),
                constraintDefinition.getConstraintType().name(),
                Iterables.asList(constraintDefinition.getPropertyKeys()),
                ""
        );
    }

    private String getSchemaInfoName(Object labelOrType, List<String> properties) {
        final String labelOrTypeAsString = labelOrType instanceof String ? (String) labelOrType : StringUtils.join(labelOrType, ",");
        return String.format(":%s(%s)", labelOrTypeAsString, StringUtils.join(properties, ","));
    }
}
