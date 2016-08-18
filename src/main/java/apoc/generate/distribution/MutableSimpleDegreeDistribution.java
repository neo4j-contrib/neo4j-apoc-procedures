package apoc.generate.distribution;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Simple {@link DegreeDistribution} where the distribution can be directly passed into the constructor.
 */
public class MutableSimpleDegreeDistribution extends SimpleDegreeDistribution implements MutableDegreeDistribution {

    public MutableSimpleDegreeDistribution(List<Integer> degrees) {
       super(degrees);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(int index, int value) {
        degrees.set(index, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void decrease(int index) {
        degrees.set(index, degrees.get(index) - 1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sort(Comparator<Integer> comparator) {
        Collections.sort(degrees, comparator);
    }
}
