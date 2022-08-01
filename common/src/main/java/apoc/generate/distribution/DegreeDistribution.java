package apoc.generate.distribution;

import java.util.List;

/**
 * A distribution of node degrees for {@link apoc.generate.relationship.RelationshipGenerator}s.
 */
public interface DegreeDistribution {

    /**
     * @return true if the config is valid.
     */
    boolean isValid();

    /**
     * Get the node degrees produced by this distribution.
     *
     * @return node degrees. Should be immutable (read-only).
     */
    List<Integer> getDegrees();

    /**
     * @return true iff this distribution is a zero-list.
     */
    boolean isZeroList();

    /**
     * Get degree by index.
     *
     * @param index of the degree to get.
     * @return degree.
     */
    int get(int index);

    /**
     * Get the size of the distribution, i.e., the number of nodes.
     *
     * @return size.
     */
    int size();
}
