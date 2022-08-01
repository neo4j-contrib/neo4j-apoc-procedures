package apoc.util;

import java.lang.reflect.Array;
import java.util.Iterator;

/**
 * @author mh
 * @since 11.06.16
 */
public class ArrayBackedIterator implements Iterator {

    private final Object array;
    private int length, cursor;

    public ArrayBackedIterator(Object array) {
        if (!array.getClass().isArray()) throw new IllegalArgumentException("Not an array " + array);
        this.array = array;
        this.length = Array.getLength(array);
    }

    @Override
    public boolean hasNext() {
        return cursor < length;
    }

    @Override
    public Object next() {
        return Array.get(array, cursor++);
    }
}
