package apoc.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ExtendedListUtils {

    /**
     * Returns a new list containing the second list appended to the
     * first list.  The {@link List#addAll(Collection)} operation is
     * used to append the two given lists into a new list.
     *
     * @param <E> the element type
     * @param list1  the first list
     * @param list2  the second list
     * @return a new list containing the union of those lists
     * @throws NullPointerException if either list is null
     */
    public static <E> List<E> union(final List<? extends E> list1, final List<? extends E> list2) {
        final ArrayList<E> result = new ArrayList<>(list1.size() + list2.size());
        result.addAll(list1);
        result.addAll(list2);
        return result;
    }
}
