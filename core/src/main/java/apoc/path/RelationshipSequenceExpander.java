package apoc.path;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.internal.helpers.collection.AbstractResourceIterable;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.helpers.collection.ResourceClosingIterator;

/**
 * An expander for repeating sequences of relationships. The sequence provided should be a string consisting of
 * relationship type/direction patterns (exactly the same as the `relationshipFilter`), separated by commas.
 * Each comma-separated pattern represents the relationships that will be expanded with each step of expansion, which
 * repeats indefinitely (unless otherwise stopped by `maxLevel`, `limit`, or terminator filtering from the other expander config options).
 * The exception is if `beginSequenceAtStart` is false. This indicates that the sequence should not begin from the start node,
 * but from one node distant. In this case, we may still need a restriction on the relationship used to reach the start node
 * of the sequence, so when `beginSequenceAtStart` is false, then the first relationship step in the sequence given will not
 * actually be used as part of the sequence, but will only be used once to reach the starting node of the sequence.
 * The remaining relationship steps will be used as the repeating relationship sequence.
 */
public class RelationshipSequenceExpander implements PathExpander {

    private final List<List<Pair<RelationshipType, Direction>>> relSequences = new ArrayList<>();
    private List<Pair<RelationshipType, Direction>> initialRels = null;

    public RelationshipSequenceExpander(String relSequenceString, boolean beginSequenceAtStart) {
        int index = 0;

        for (String sequenceStep : relSequenceString.split(",")) {
            sequenceStep = sequenceStep.trim();
            Iterable<Pair<RelationshipType, Direction>> relDirIterable = RelationshipTypeAndDirections.parse(sequenceStep);

            List<Pair<RelationshipType, Direction>> stepRels = new ArrayList<>();
            for (Pair<RelationshipType, Direction> pair : relDirIterable) {
                stepRels.add(pair);
            }

            if (!beginSequenceAtStart && index == 0) {
                initialRels = stepRels;
            } else {
                relSequences.add(stepRels);
            }

            index++;
        }
    }

    public RelationshipSequenceExpander(List<String> relSequenceList, boolean beginSequenceAtStart) {
        int index = 0;

        for (String sequenceStep : relSequenceList) {
            sequenceStep = sequenceStep.trim();
            Iterable<Pair<RelationshipType, Direction>> relDirIterable = RelationshipTypeAndDirections.parse(sequenceStep);

            List<Pair<RelationshipType, Direction>> stepRels = new ArrayList<>();
            for (Pair<RelationshipType, Direction> pair : relDirIterable) {
                stepRels.add(pair);
            }

            if (!beginSequenceAtStart && index == 0) {
                initialRels = stepRels;
            } else {
                relSequences.add(stepRels);
            }

            index++;
        }
    }

    @Override
    public ResourceIterable<Relationship> expand( Path path, BranchState state ) {
        final Node node = path.endNode();
        int depth = path.length();
        List<Pair<RelationshipType, Direction>> stepRels;

        if (depth == 0 && initialRels != null) {
            stepRels = initialRels;
        } else {
            stepRels = relSequences.get((initialRels == null ? depth : depth - 1) % relSequences.size());
        }

        return new AbstractResourceIterable<>() {
            @Override
            protected ResourceIterator<Relationship> newIterator() {
                return Iterators.concatResourceIterators(stepRels.stream().map(entry -> {
                   RelationshipType type = entry.first();
                   Direction dir = entry.other();

                   ResourceIterable<Relationship> relationships;
                   if (type == null) {
                       relationships = (dir == Direction.BOTH) ? node.getRelationships() : node.getRelationships(dir);
                   } else {
                       relationships = (dir == Direction.BOTH) ? node.getRelationships(type) : node.getRelationships(dir, type);
                   }

                   return ResourceClosingIterator.fromResourceIterable(relationships);
                }).iterator());
            }
        };
    }

    @Override
    public PathExpander reverse() {
        throw new RuntimeException("Not implemented");
    }
}
