package apoc.schema;

import apoc.result.CompareIdxToCons;
import apoc.result.IndexConstraintNodeInfo;
import apoc.result.IndexConstraintRelationshipInfo;
import apoc.util.collection.Iterables;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.KernelTransaction;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static org.apache.arrow.vector.dictionary.DictionaryEncoder.getIndexType;
import static org.neo4j.internal.schema.SchemaUserDescription.TOKEN_LABEL;
import static org.neo4j.internal.schema.SchemaUserDescription.TOKEN_REL_TYPE;

public class SchemasExtendedUtil {
    public static final String IDX_NOT_FOUND = "NOT_FOUND";

    public static IndexConstraintNodeInfo nodeInfoFromIndexDefinition(
            IndexDescriptor indexDescriptor, SchemaRead schemaRead, TokenNameLookup tokens, Boolean useStoredName) {
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
                    useStoredName ? indexDescriptor.getName() : schemaInfoName,
                    labelName,
                    properties,
                    schemaRead.indexGetState(indexDescriptor).toString(),
                    getIndexType(indexDescriptor),
                    schemaRead.indexGetState(indexDescriptor).equals(InternalIndexState.FAILED)
                            ? schemaRead.indexGetFailure(indexDescriptor)
                            : "NO FAILURE",
                    getPopulationProgress(indexDescriptor, schemaRead),
                    schemaRead.indexSize(indexDescriptor),
                    schemaRead.indexUniqueValuesSelectivity(indexDescriptor),
                    userDescription);
        } catch (IndexNotFoundKernelException e) {
            return new IndexConstraintNodeInfo(
                    schemaInfoName,
                    labelName,
                    properties,
                    IDX_NOT_FOUND,
                    getIndexType(indexDescriptor),
                    IDX_NOT_FOUND,
                    0,
                    0,
                    0,
                    userDescription);
        }
    }

    public static IndexConstraintRelationshipInfo relationshipInfoFromIndexDescription(
            IndexDescriptor indexDescriptor, TokenNameLookup tokens, SchemaRead schemaRead, Boolean useStoredName) {
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
        final String name = useStoredName ? indexDescriptor.getName() : getSchemaInfoName(relName, properties);
        final String schemaType = getIndexType(indexDescriptor);

        String indexStatus;
        try {
            indexStatus = schemaRead.indexGetState(indexDescriptor).toString();
        } catch (IndexNotFoundKernelException e) {
            indexStatus = IDX_NOT_FOUND;
        }

        return new IndexConstraintRelationshipInfo(name, schemaType, properties, indexStatus, relName);
    }

    public static String getIndexType(IndexDescriptor indexDescriptor) {
        return indexDescriptor.getIndexType().name();
    }

    public static IndexConstraintRelationshipInfo relationshipInfoFromConstraintDefinition(
            ConstraintDefinition constraintDefinition, Boolean useStoredName) {
        return new IndexConstraintRelationshipInfo(
                useStoredName
                        ? constraintDefinition.getName()
                        : String.format("CONSTRAINT %s", constraintDefinition.toString()),
                constraintDefinition.getConstraintType().name(),
                Iterables.asList(constraintDefinition.getPropertyKeys()),
                "",
                constraintDefinition.getRelationshipType().name());
    }
    public static long getPopulationProgress(IndexDescriptor indexDescriptor, SchemaRead schemaRead)
            throws IndexNotFoundKernelException {
        PopulationProgress populationProgress = schemaRead.indexGetPopulationProgress(indexDescriptor);
        // when the index is failed the getTotal() is equal to 0
        long populationTotal = populationProgress.getTotal();
        if (populationTotal == 0) {
            return 0L;
        }
        return populationProgress.getCompleted() / populationTotal * 100;
    }


    public static String getSchemaInfoName(Object labelOrType, List<String> properties) {
        final String labelOrTypeAsString =
                labelOrType instanceof String ? (String) labelOrType : StringUtils.join(labelOrType, ",");
        return String.format(":%s(%s)", labelOrTypeAsString, StringUtils.join(properties, ","));
    }
    /**
     * ConstraintInfo info from ConstraintDefinition
     *
     * @param constraintDefinition
     * @param tokens
     * @return
     */
    public static IndexConstraintNodeInfo nodeInfoFromConstraintDefinition(
            ConstraintDefinition constraintDefinition, TokenNameLookup tokens, Boolean useStoredName, KernelTransaction ktx) {
        String labelName = constraintDefinition.getLabel().name();
        List<String> properties = Iterables.asList(constraintDefinition.getPropertyKeys());
        return new IndexConstraintNodeInfo(
                // Pretty print for index name
                useStoredName
                        ? constraintDefinition.getName()
                        : String.format(":%s(%s)", labelName, StringUtils.join(properties, ",")),
                labelName,
                properties,
                StringUtils.EMPTY,
                constraintDefinition.getConstraintType().name(),
                "NO FAILURE",
                0,
                0,
                0,
                nodeConstraintCypher5Compatibility(
                        ktx.schemaRead()
                                .constraintGetForName(constraintDefinition.getName())
                                .userDescription(tokens),
                        useStoredName));
    }

    public static String nodeConstraintCypher5Compatibility(String userDescription, Boolean useStoredName) {
        if (useStoredName) {
            return userDescription;
        } else {
            // Revert to old description on Cypher 5 for backwards compatibility.
            return userDescription.replace("'NODE PROPERTY UNIQUENESS'", "'UNIQUENESS'");
        }
    }
    public static List<IndexDescriptor> getIndexesFromSchema(
            Iterator<IndexDescriptor> allIndex, Predicate<IndexDescriptor> indexDescriptorPredicate) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(allIndex, Spliterator.ORDERED), false)
                .filter(indexDescriptorPredicate)
                .collect(Collectors.toList());
    }

    public static Object getInfoLabelOrType(Object idxOrCons) {
        if (idxOrCons instanceof IndexConstraintNodeInfo) {
            return ((IndexConstraintNodeInfo) idxOrCons).label;
        }
        return ((IndexConstraintRelationshipInfo) idxOrCons).relationshipType;
    }

    public static <T extends CompareIdxToCons> T addObjectIfAbsent(Map<String, T> map, String label, Class<T> clazz) {
        return map.compute(label,
                (k, v) -> Objects.requireNonNullElseGet(v, () -> {
                    try {
                        return clazz.getConstructor(String.class).newInstance(label);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
    }
}
