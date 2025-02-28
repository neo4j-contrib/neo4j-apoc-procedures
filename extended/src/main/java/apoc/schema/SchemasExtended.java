package apoc.schema;

import apoc.Extended;
import apoc.result.*;
import apoc.util.CollectionUtils;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.procedure.*;
import org.neo4j.token.api.TokenConstants;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.schema.SchemasExtendedUtil.*;
import static org.apache.arrow.vector.dictionary.DictionaryEncoder.getIndexType;
import static org.neo4j.graphdb.schema.ConstraintType.UNIQUENESS;
import static org.neo4j.internal.schema.SchemaUserDescription.TOKEN_LABEL;
import static org.neo4j.internal.schema.SchemaUserDescription.TOKEN_REL_TYPE;

@Extended
public class SchemasExtended {

    @Context
    public Transaction tx;

    @Context
    public KernelTransaction ktx;

    @Procedure(value = "apoc.schema.node.compareIndexesAndConstraints", mode = Mode.SCHEMA)
    @Description("CALL apoc.schema.node.compareIndexesAndConstraints($config) - to compare node constraints and indexes")
    public Stream<CompareIdxToConsNodes> compareIndexesAndConstraints(@Name(value = "config",defaultValue = "{}") Map<String,Object> config) {
        return indexesAndConstraintsForNode(config, tx, ktx,
                compareConstraintIdxFunction(CompareIdxToConsNodes.class));
    }

    @Procedure(value = "apoc.schema.relationship.compareIndexesAndConstraints", mode = Mode.SCHEMA)
    @Description("CALL apoc.schema.relationship.compareIndexesAndConstraints($config) - to compare rel constraints and indexes")
    public Stream<CompareIdxToConsRels> compareIndexesAndConstraintsForRelationships(@Name(value = "config",defaultValue = "{}") Map<String,Object> config) {
        return indexesAndConstraintsForRelationships(config, tx, ktx, compareConstraintIdxFunction(CompareIdxToConsRels.class));
    }
    


    private static <R extends CompareIdxToCons> BiFunction<Stream<Object>, Stream<Object>, Stream<R>> compareConstraintIdxFunction(Class<R> clazz) {
        return (constraintNodeInfoStream, indexNodeInfoStream) -> {
            final List<Object> constraints = constraintNodeInfoStream.toList();
            final List<Object> indexes = indexNodeInfoStream.toList();

            Map<String, R> resultMap = new TreeMap<>();

            indexes.forEach(i -> {
                final Object labelOrType = getInfoLabelOrType(i);
                if (labelOrType instanceof String) {
                    addCommonAndOnlyIdxProps(constraints, addObjectIfAbsent(resultMap, (String) labelOrType, clazz), i);
                }
                if (labelOrType instanceof List) {
                    final List<String> labels = (List<String>) labelOrType;
                    labels.forEach(lbl -> {
                        addCommonAndOnlyIdxProps(constraints, addObjectIfAbsent(resultMap, lbl, clazz), i);
                    });
                }
            });

            constraints.forEach(i -> {
                List<String> props = new ArrayList<>();
                String type = null;
                String name = null;
                if (clazz.isAssignableFrom(IndexConstraintNodeInfo.class)) {
                    IndexConstraintNodeInfo info = (IndexConstraintNodeInfo) i;
                    props = info.properties;
                    type = info.type;
                    name = info.name;
                }
                if (clazz.isAssignableFrom(IndexConstraintRelationshipInfo.class)) {
                    IndexConstraintRelationshipInfo info = (IndexConstraintRelationshipInfo) i;
                    props = info.properties;
                    name = info.name;
                }
                final Object labelOrType = getInfoLabelOrType(i);
                addObjectIfAbsent(resultMap, (String) labelOrType, clazz).putOnlyConstraintsProps(name, props);
            });

            return resultMap.values().stream();
        };
    }

    private static void addCommonAndOnlyIdxProps(List<Object> constraints, CompareIdxToCons compareIdxToCons, Object index) {
        List<String> props = new ArrayList<>();
        String type = null;
        String name = null;
        if (index instanceof IndexConstraintNodeInfo info) {
            props = info.properties;
            type = info.type;
            name = info.name;
        } 
        if (index instanceof IndexConstraintRelationshipInfo info) {
            props = info.properties;
            type = info.type;
            name = info.name;
        }

        // UNIQUENESS constraints also produce an analogous index, so the properties is necessary in common
        if (UNIQUENESS.name().equals(type)) {
            compareIdxToCons.addCommonProps(props);
        } else {
            List<String> finalProps = props;
            String finalName = name;
            constraints.stream()
                    .filter(cons -> {
                        final Object idxLabelOrType = getInfoLabelOrType(index);
                        final Object constraintLabelOrType = getInfoLabelOrType(cons);

                        List<String> consProps = new ArrayList<>();
                        List<String> indexProps = new ArrayList<>();
                        if (index instanceof IndexConstraintNodeInfo info) {
                            indexProps = info.properties;
                            consProps = ((IndexConstraintNodeInfo) cons).properties;
                        } else if (index instanceof IndexConstraintRelationshipInfo info) {
                            indexProps = info.properties;
                            consProps = ((IndexConstraintRelationshipInfo) cons).properties;
                        }

                        return idxLabelOrType.equals(constraintLabelOrType)
                                && indexProps != null
                                && consProps != null
                                && CollectionUtils.isEqualCollection(indexProps, consProps);
                    })
                    .findFirst()
                    .ifPresentOrElse(pres -> {
                                compareIdxToCons.addCommonProps(finalProps);
                                constraints.remove(pres);
                            },
                            () -> compareIdxToCons.putOnlyIdxProps(finalName, finalProps)
                    );
        }
    }

    public static <T> T indexesAndConstraintsForNode(Map<String,Object> config, Transaction tx, KernelTransaction ktx, BiFunction<Stream<Object>, Stream<Object>, T> function) {
        Schema schema = tx.schema();

        SchemaConfigExtended schemaConfig = new SchemaConfigExtended(config);
        Set<String> includeLabels = schemaConfig.getLabels();
        Set<String> excludeLabels = schemaConfig.getExcludeLabels();

        try (Statement ignore = ktx.acquireStatement()) {
            TokenRead tokenRead = ktx.tokenRead();

            SchemaRead schemaRead = ktx.schemaRead();
            Iterable<IndexDescriptor> indexesIterator;
            Iterable<ConstraintDefinition> constraintsIterator;
            final Predicate<ConstraintDefinition> isNodeConstraint =
                    constraintDefinition -> Util.isNodeCategory(constraintDefinition.getConstraintType());

            if (includeLabels.isEmpty()) {

                Iterator<IndexDescriptor> allIndex = schemaRead.indexesGetAll();

                indexesIterator = getIndexesFromSchema(
                        allIndex,
                        index -> index.schema().entityType().equals(EntityType.NODE)
                                && Arrays.stream(index.schema().getEntityTokenIds())
                                .noneMatch(id -> {
                                    try {
                                        return excludeLabels.contains(tokenRead.nodeLabelName(id));
                                    } catch (LabelNotFoundKernelException e) {
                                        return false;
                                    }
                                }));

                Iterable<ConstraintDefinition> allConstraints = schema.getConstraints();
                constraintsIterator = StreamSupport.stream(allConstraints.spliterator(), false)
                        .filter(isNodeConstraint)
                        .filter(constraint ->
                                !excludeLabels.contains(constraint.getLabel().name()))
                        .collect(Collectors.toList());
            } else {
                constraintsIterator = includeLabels.stream()
                        .filter(label -> !excludeLabels.contains(label) && tokenRead.nodeLabel(label) != -1)
                        .flatMap(label -> {
                            Iterable<ConstraintDefinition> constraintsForType =
                                    schema.getConstraints(Label.label(label));
                            return StreamSupport.stream(constraintsForType.spliterator(), false)
                                    .filter(isNodeConstraint);
                        })
                        .collect(Collectors.toList());

                indexesIterator = includeLabels.stream()
                        .filter(label -> !excludeLabels.contains(label) && tokenRead.nodeLabel(label) != -1)
                        .flatMap(label -> {
                            Iterable<IndexDescriptor> indexesForLabel =
                                    () -> schemaRead.indexesGetForLabel(tokenRead.nodeLabel(label));
                            return StreamSupport.stream(indexesForLabel.spliterator(), false);
                        })
                        .collect(Collectors.toList());
            }

            Stream<Object> constraintNodeInfoStream = StreamSupport.stream(
                            constraintsIterator.spliterator(), false)
                    .map(constraintDescriptor ->
                            nodeInfoFromConstraintDefinition(constraintDescriptor, tokenRead, false, ktx))
                    .sorted(Comparator.comparing(i -> i.label.toString()))
                    .map(x -> (Object) x);

            Stream<Object> indexNodeInfoStream = StreamSupport.stream(
                            indexesIterator.spliterator(), false)
                    .map(indexDescriptor ->
                            nodeInfoFromIndexDefinition(indexDescriptor, schemaRead, tokenRead, false))
                    .sorted(Comparator.comparing(i -> i.label.toString()))
                    .map(x -> (Object) x);

            return function.apply(constraintNodeInfoStream, indexNodeInfoStream);
        }
    }

    public static  <T> T indexesAndConstraintsForRelationships(Map<String,Object> config, Transaction tx, KernelTransaction ktx, BiFunction<Stream<Object>, Stream<Object>, T> function) {
        Schema schema = tx.schema();

        SchemaConfigExtended schemaConfig = new SchemaConfigExtended(config);
        Set<String> includeRelationships = schemaConfig.getRelationships();
        Set<String> excludeRelationships = schemaConfig.getExcludeRelationships();

        try (Statement ignore = ktx.acquireStatement()) {
            TokenRead tokenRead = ktx.tokenRead();
            SchemaRead schemaRead = ktx.schemaRead();
            Iterable<ConstraintDefinition> constraintsIterator;
            Iterable<IndexDescriptor> indexesIterator;

            final Predicate<ConstraintDefinition> isRelConstraint =
                    constraintDefinition -> Util.isRelationshipCategory(constraintDefinition.getConstraintType());

            if (!includeRelationships.isEmpty()) {
                constraintsIterator = includeRelationships.stream()
                        .filter(type -> !excludeRelationships.contains(type)
                                && tokenRead.relationshipType(type) != TokenConstants.NO_TOKEN)
                        .flatMap(type -> {
                            Iterable<ConstraintDefinition> constraintsForType =
                                    schema.getConstraints(RelationshipType.withName(type));
                            return StreamSupport.stream(constraintsForType.spliterator(), false)
                                    .filter(isRelConstraint);
                        })
                        .collect(Collectors.toList());

                indexesIterator = includeRelationships.stream()
                        .filter(type -> !excludeRelationships.contains(type)
                                && tokenRead.relationshipType(type) != TokenConstants.NO_TOKEN)
                        .flatMap(type -> {
                            Iterable<IndexDescriptor> indexesForRelType =
                                    () -> schemaRead.indexesGetForRelationshipType(tokenRead.relationshipType(type));
                            return StreamSupport.stream(indexesForRelType.spliterator(), false);
                        })
                        .collect(Collectors.toList());
            } else {
                Iterable<ConstraintDefinition> allConstraints = schema.getConstraints();
                constraintsIterator = StreamSupport.stream(allConstraints.spliterator(), false)
                        .filter(isRelConstraint)
                        .filter(constraint -> !excludeRelationships.contains(
                                constraint.getRelationshipType().name()))
                        .collect(Collectors.toList());

                Iterator<IndexDescriptor> allIndex = schemaRead.indexesGetAll();
                indexesIterator = getIndexesFromSchema(
                        allIndex,
                        index -> index.schema().entityType().equals(EntityType.RELATIONSHIP)
                                && Arrays.stream(index.schema().getEntityTokenIds())
                                .noneMatch(id ->
                                        excludeRelationships.contains(tokenRead.relationshipTypeGetName(id))));
            }

            Stream<Object> constraintRelationshipInfoStream = StreamSupport.stream(
                            constraintsIterator.spliterator(), false)
                    .map(c -> relationshipInfoFromConstraintDefinition(c, true));

            Stream<Object> indexRelationshipInfoStream = StreamSupport.stream(
                            indexesIterator.spliterator(), false)
                    .map(index -> relationshipInfoFromIndexDescription(index, tokenRead, schemaRead, true));

            return function.apply(constraintRelationshipInfoStream, indexRelationshipInfoStream);
        }
    }

}
