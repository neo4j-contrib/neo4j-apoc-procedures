package apoc.util;

import java.lang.reflect.Array;
import java.util.AbstractList;

/**
 * @author mh
 * @since 11.06.16
 */
public class ArrayBackedList extends AbstractList {

    private final Object array;
    private int length;

    public ArrayBackedList(Object array) {
        if (!array.getClass().isArray()) throw new IllegalArgumentException("Not an array " + array);
        this.array = array;
        this.length = Array.getLength(array);
    }

    @Override
    public Object get(int index) {
        return Array.get(array, index);
    }

    @Override
    public int size() {
        return length;
    }
}
