package apoc.sequence;

import java.util.LinkedList;

/**
 * Created by Valentino Maiorca on 6/29/17.
 */
public interface SequenceProvider<V> {

    String SEQUENCE_LABEL_PROPERTY = "next_value";

    default String propertyName() {
        return SEQUENCE_LABEL_PROPERTY;
    }

    /**
     * Returns the start value for this sequence.
     */
    V start();

    /**
     * Returns the value next to the one provided.
     *
     * @param current
     * @return
     */
    V next(V current);

    /**
     * @return The name for the sequence.
     */
    String getName();

    /**
     * Returns a list of values next to the one provided (not included).
     * @param current The start value to compute from.
     * @param length The list size.
     * @return The next values list.
     */
    default LinkedList<V> next(V current, long length) {
        LinkedList<V> result = new LinkedList<>();

        for (int i = 0; i < length; i++)
            result.add(current = next(current));

        return result;
    }
}
