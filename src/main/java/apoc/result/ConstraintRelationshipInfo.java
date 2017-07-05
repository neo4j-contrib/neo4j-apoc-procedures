package apoc.result;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.storageengine.api.RelationshipItem;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by alberto.delazzari on 04/07/17.
 */
public class ConstraintRelationshipInfo {

    public final String name;

    public final List<String> startLabel;

    public final String type;

    public final List<String> endLabel;

    public final List<String> properties;

    public final String status;

    public ConstraintRelationshipInfo(ReadOperations readOperations, RelationshipPropertyConstraint propertyConstraint) throws Exception {
        StatementTokenNameLookup lookup = new StatementTokenNameLookup(readOperations);

        name = propertyConstraint.userDescription(lookup);
        properties = Collections.singletonList(lookup.propertyKeyGetName(propertyConstraint.propertyKey()));
        status = "";
        type = lookup.relationshipTypeGetName(propertyConstraint.relationshipType());

        LabelResult labelResult = getNodeLabelsFromRelationshipTypeId(readOperations, propertyConstraint.relationshipType());

        startLabel = labelResult.startLabels;
        endLabel = labelResult.endLabels;
    }

    private LabelResult getNodeLabelsFromRelationshipTypeId(ReadOperations readOperations, int relationshipTypeId)
            throws LabelNotFoundKernelException, RelationshipTypeIdNotFoundKernelException, EntityNotFoundException {
        // Iterate all over the relationships
        PrimitiveLongIterator relationshipIterator = readOperations.relationshipsGetAll();

        List<String> startNodeLabels = new ArrayList<>();
        List<String> endNodeLabels = new ArrayList<>();

        while (relationshipIterator.hasNext()) {
            long relId = relationshipIterator.next();

            Cursor<RelationshipItem> relationshipItemCursor = readOperations.relationshipCursor(relId);
            if (relationshipItemCursor.next()) {
                // Given the relationship item
                RelationshipItem relationshipItem = relationshipItemCursor.get();

                // This relationship type equals to the second parameter
                if (relationshipTypeId == relationshipItem.type()) {
                    // Label ids for the start node
                    PrimitiveIntIterator primitiveIntIterator = readOperations.nodeGetLabels(relationshipItem.startNode());

                    while (primitiveIntIterator.hasNext()) {
                        int labelId = primitiveIntIterator.next();
                        startNodeLabels.add(readOperations.labelGetName(labelId));
                    }

                    // Label ids for the end node
                    primitiveIntIterator = readOperations.nodeGetLabels(relationshipItem.endNode());
                    while (primitiveIntIterator.hasNext()) {
                        int labelId = primitiveIntIterator.next();
                        endNodeLabels.add(readOperations.labelGetName(labelId));
                    }
                } else {
                    continue;
                }
            }

            relationshipItemCursor.close();
        }

        return new LabelResult(startNodeLabels, endNodeLabels);
    }

    private class LabelResult {
        public final List<String> startLabels;
        public final List<String> endLabels;

        public LabelResult(List<String> startLabels, List<String> endLabels) {
            this.startLabels = startLabels;
            this.endLabels = endLabels;
        }
    }
}
