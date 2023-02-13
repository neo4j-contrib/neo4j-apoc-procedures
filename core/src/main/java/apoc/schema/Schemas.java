/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.schema;

import apoc.result.AssertSchemaResult;
import apoc.result.IndexConstraintNodeInfo;
import apoc.result.IndexConstraintRelationshipInfo;
import apoc.util.Util;
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
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.schema.ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE;
import static org.neo4j.internal.schema.SchemaUserDescription.TOKEN_LABEL;
import static org.neo4j.internal.schema.SchemaUserDescription.TOKEN_REL_TYPE;

public class Schemas {
    private static final String IDX_NOT_FOUND = "NOT_FOUND";

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
            ConstraintType constraintType = definition.getConstraintType();
            String label = isRelConstraint(definition) ? definition.getRelationshipType().name() : definition.getLabel().name();
            AssertSchemaResult info = new AssertSchemaResult(label, Iterables.asList(definition.getPropertyKeys()));
            if (Util.constraintIsUnique(constraintType)) {
                info = info.unique();
            }
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
                .map( property -> String.format("n.`%s`", Util.sanitize(property.toString())))
                .collect( Collectors.joining( "," ) );
        tx.execute(String.format("CREATE CONSTRAINT ON (n:`%s`) ASSERT (%s) IS NODE KEY", Util.sanitize(lbl), keyProperties)).close();
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
            if (definition.getIndexType() == IndexType.LOOKUP)
                continue;
            if (definition.isConstraintIndex())
                continue;
            if (definition.isMultiTokenIndex())
                continue;

            Object label = getLabelForAssert(definition, definition.isNodeIndex());
            List<String> keys = new ArrayList<>();
            definition.getPropertyKeys().forEach(keys::add);

            AssertSchemaResult info = new AssertSchemaResult(label, keys);

            final boolean included = Optional.ofNullable(indexes.get(label))
                    .map(lbl -> {
                        if (keys.size() > 1) {
                            return lbl.remove(keys);
                        }
                        if (keys.size() == 1) {
                            return lbl.remove(keys.get(0));
                        }
                        // todo - it shouldn't be needed. only LOOKUP indexes, absent in 4.2 and previous and filtered for 4.3+, can be without keys
                        throw new IllegalArgumentException("Label given with no keys.");
                    })
                    .orElse(false);

            if (dropExisting && !included) {
                definition.drop();
                info.dropped();
            }

            result.add(info);
        }


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
        keys.forEach(key->backTickedKeys.add(String.format("`%s`", Util.sanitize(key))));

        tx.execute(String.format("CREATE INDEX ON :`%s` (%s)", Util.sanitize(label), String.join(",", backTickedKeys))).close();
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
        Schema schema = tx.schema();

        SchemaConfig schemaConfig = new SchemaConfig(config);
        Set<String> includeLabels = schemaConfig.getLabels();
        Set<String> excludeLabels = schemaConfig.getExcludeLabels();

        try ( Statement ignore = ktx.acquireStatement() ) {
            TokenRead tokenRead = ktx.tokenRead();

            SchemaRead schemaRead = ktx.schemaRead();
            Iterable<IndexDescriptor> indexesIterator;
            Iterable<ConstraintDefinition> constraintsIterator;
            final Predicate<ConstraintDefinition> isNodeConstraint = constraintDefinition ->
                    !isRelConstraint(constraintDefinition);

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

                Iterable<ConstraintDefinition> allConstraints = schema.getConstraints();
                constraintsIterator = StreamSupport.stream(allConstraints.spliterator(),false)
                        .filter(isNodeConstraint)
                        .filter(constraint -> !excludeLabels.contains(constraint.getLabel().name()))
                        .collect(Collectors.toList());
            } else {
                constraintsIterator = includeLabels.stream()
                        .filter(label -> !excludeLabels.contains(label) && tokenRead.nodeLabel(label) != -1)
                        .flatMap(label -> {
                            Iterable<ConstraintDefinition> constraintsForType = schema.getConstraints(Label.label(label));
                            return StreamSupport.stream(constraintsForType.spliterator(), false)
                                    .filter(isNodeConstraint);
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
                    .map(constraintDescriptor -> nodeInfoFromConstraintDefinition(constraintDescriptor, tokenRead))
                    .sorted(Comparator.comparing(i -> i.label.toString()));

            Stream<IndexConstraintNodeInfo> indexNodeInfoStream = StreamSupport.stream(indexesIterator.spliterator(), false)
                    .map(indexDescriptor -> this.nodeInfoFromIndexDefinition(indexDescriptor, schemaRead, tokenRead))
                    .sorted(Comparator.comparing(i -> i.label.toString()));

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

            final Predicate<ConstraintDefinition> isRelConstraint = this::isRelConstraint;

            if(!includeRelationships.isEmpty()) {
                constraintsIterator = includeRelationships.stream()
                        .filter(type -> !excludeRelationships.contains(type) && tokenRead.relationshipType(type) != TokenConstants.NO_TOKEN)
                        .flatMap(type -> {
                            Iterable<ConstraintDefinition> constraintsForType = schema.getConstraints(RelationshipType.withName(type));
                            return StreamSupport.stream(constraintsForType.spliterator(), false)
                                    .filter(isRelConstraint);
                        })
                        .collect(Collectors.toList());

                indexesIterator = includeRelationships.stream()
                        .filter(type -> !excludeRelationships.contains(type) && tokenRead.relationshipType(type) != TokenConstants.NO_TOKEN)
                        .flatMap(type -> {
                            Iterable<IndexDescriptor> indexesForRelType = () -> schemaRead.indexesGetForRelationshipType(tokenRead.relationshipType(type));
                            return StreamSupport.stream(indexesForRelType.spliterator(), false);
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
                        .filter(isRelConstraint)
                        .filter(constraint -> !excludeRelationships.contains(constraint.getRelationshipType().name()))
                        .collect(Collectors.toList());

                Iterator<IndexDescriptor> allIndex = schemaRead.indexesGetAll();
                indexesIterator = getIndexesFromSchema(allIndex, index -> index.schema().entityType().equals(EntityType.RELATIONSHIP)
                        && Arrays.stream(index.schema().getEntityTokenIds())
                        .noneMatch(id -> excludeRelationships.contains(tokenRead.relationshipTypeGetName(id))));
            }

            Stream<IndexConstraintRelationshipInfo> constraintRelationshipInfoStream = StreamSupport.stream(constraintsIterator.spliterator(), false)
                    .map(this::relationshipInfoFromConstraintDefinition);

            Stream<IndexConstraintRelationshipInfo> indexRelationshipInfoStream = StreamSupport.stream(indexesIterator.spliterator(), false)
                    .map(index -> relationshipInfoFromIndexDescription(index, tokenRead, schemaRead));

            return Stream.of(constraintRelationshipInfoStream, indexRelationshipInfoStream).flatMap(e -> e);
        }
    }

    private boolean isRelConstraint(ConstraintDefinition constraintDefinition) {
        return constraintDefinition.isConstraintType(RELATIONSHIP_PROPERTY_EXISTENCE);
    }

    /**
     * ConstraintInfo info from ConstraintDefinition
     *
     * @param constraintDefinition
     * @param tokens
     * @return
     */
    private IndexConstraintNodeInfo nodeInfoFromConstraintDefinition(ConstraintDefinition constraintDefinition, TokenNameLookup tokens) {
        String labelName = constraintDefinition.getLabel().name();
        List<String> properties = Iterables.asList(constraintDefinition.getPropertyKeys());
        return new IndexConstraintNodeInfo(
                // Pretty print for index name
                String.format(":%s(%s)", labelName, StringUtils.join(properties, ",")),
                labelName,
                properties,
                StringUtils.EMPTY,
                constraintDefinition.getConstraintType().name(),
                "NO FAILURE",
                0,
                0,
                0,
                ktx.schemaRead().constraintGetForName(constraintDefinition.getName()).userDescription(tokens)
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
        final Object labelName;
        if (length == 0) {
            labelName = TOKEN_LABEL;
        } else {
            final List<String> labels = IntStream.of(labelIds)
                    .mapToObj(tokens::labelGetName)
                    .sorted()
                    .collect(Collectors.toList());
            labelName = labels.size() > 1 ? labels : labels.get(0);
        }
        // to handle LOOKUP indexes
        List<String> properties = IntStream.of(indexDescriptor.schema().getPropertyIds())
                .mapToObj(tokens::propertyKeyGetName)
                .collect(Collectors.toList());

        // Pretty print for index name
        final String schemaInfoName = getSchemaInfoName(labelName, properties);
        final String userDescription = indexDescriptor.userDescription(tokens);
        try {
            return new IndexConstraintNodeInfo(
                    schemaInfoName,
                    labelName,
                    properties,
                    schemaRead.indexGetState(indexDescriptor).toString(),
                    getIndexType(indexDescriptor),
                    schemaRead.indexGetState(indexDescriptor).equals(InternalIndexState.FAILED) ? schemaRead.indexGetFailure(indexDescriptor) : "NO FAILURE",
                    getPopulationProgress(indexDescriptor, schemaRead),
                    schemaRead.indexSize(indexDescriptor),
                    schemaRead.indexUniqueValuesSelectivity(indexDescriptor),
                    userDescription
            );
        } catch(IndexNotFoundKernelException e) {
            return new IndexConstraintNodeInfo(
                    schemaInfoName,
                    labelName,
                    properties,
                    IDX_NOT_FOUND,
                    getIndexType(indexDescriptor),
                    IDX_NOT_FOUND,
                    0,0,0,
                    userDescription
            );
        }
    }

    private IndexConstraintRelationshipInfo relationshipInfoFromIndexDescription(IndexDescriptor indexDescriptor, TokenNameLookup tokens, SchemaRead schemaRead) {
        int[] relIds = indexDescriptor.schema().getEntityTokenIds();
        int length = relIds.length;
        // to handle LOOKUP indexes
        final Object relName;
        if (length == 0) {
            relName = TOKEN_REL_TYPE;
        } else {
            final List<String> rels = IntStream.of(relIds)
                    .mapToObj(tokens::relationshipTypeGetName)
                    .sorted()
                    .collect(Collectors.toList());
            relName = rels.size() > 1 ? rels : rels.get(0);
        }
        final List<String> properties = Arrays.stream(indexDescriptor.schema().getPropertyIds())
                .mapToObj(tokens::propertyKeyGetName)
                .collect(Collectors.toList());

        // Pretty print for index name
        final String name = getSchemaInfoName(relName, properties);
        final String schemaType = getIndexType(indexDescriptor);

        String indexStatus;
        try {
            indexStatus = schemaRead.indexGetState(indexDescriptor).toString();
        } catch (IndexNotFoundKernelException e) {
            indexStatus = IDX_NOT_FOUND;
        }

        return new IndexConstraintRelationshipInfo(name, schemaType, properties, indexStatus, relName);
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
                "",
                constraintDefinition.getRelationshipType().name()
        );
    }

    private static String getIndexType(IndexDescriptor indexDescriptor) {
        return indexDescriptor.getIndexType().name();
    }

    private String getSchemaInfoName(Object labelOrType, List<String> properties) {
        final String labelOrTypeAsString = labelOrType instanceof String ? (String) labelOrType : StringUtils.join(labelOrType, ",");
        return String.format(":%s(%s)", labelOrTypeAsString, StringUtils.join(properties, ","));
    }

    private Object getLabelForAssert(IndexDefinition definition, boolean nodeIndex) {
        if (nodeIndex) {
            return definition.isMultiTokenIndex()
                    ? Iterables.stream(definition.getLabels()).map(Label::name).collect(Collectors.toList())
                    : Iterables.single(definition.getLabels()).name();
        } else {
            return definition.isMultiTokenIndex()
                    ? Iterables.stream(definition.getRelationshipTypes()).map(RelationshipType::name).collect(Collectors.toList())
                    : Iterables.single(definition.getRelationshipTypes()).name();
        }
    }

    private long getPopulationProgress(IndexDescriptor indexDescriptor, SchemaRead schemaRead) throws IndexNotFoundKernelException {
        PopulationProgress populationProgress = schemaRead.indexGetPopulationProgress(indexDescriptor);
        // when the index is failed the getTotal() is equal to 0
        long populationTotal = populationProgress.getTotal();
        if (populationTotal == 0) {
            return 0L;
        }
        return populationProgress.getCompleted() / populationTotal * 100;
    }
}
