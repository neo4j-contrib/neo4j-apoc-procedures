package apoc.generate.distribution;

import java.util.Comparator;

/**
 * A mutable {@link DegreeDistribution}.
 */
public interface MutableDegreeDistribution extends DegreeDistribution {

    /**
     * Set degree by index.
     *
     * @param index of the degree to set.
     * @param value to set.
     */
    void set(int index, int value);

    /**
     * Decrease the degree at the specified index by 1.
     *
     * @param index to decrease.
     */
    void decrease(int index);

    /**
     * Sort the degree distribution using the given comparator.
     *
     * @param comparator comparator.
     */
    void sort(Comparator<Integer> comparator);
}
