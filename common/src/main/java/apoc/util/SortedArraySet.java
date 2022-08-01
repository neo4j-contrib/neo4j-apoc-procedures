package apoc.util;

import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * @author mh
 * @since 22.06.17
 */
public class SortedArraySet<T extends Comparable<T>> {
    private static final int GROWTH = 10;
    T[] data;
    private int capacity;
    private int size;

    public SortedArraySet(Class<T> type, int capacity) {
        this.capacity = capacity;
        this.size = 0;
        //noinspection unchecked
        this.data = (T[]) Array.newInstance(type, capacity);
    }

    public T get(int idx) {

        return data[idx];
    }

    public T find(T key) {
        if (key == null) throw new IllegalArgumentException("lookup key must not be null");
        if (size == 0) return null;
        int offset = Arrays.binarySearch(data, 0, size, key);
        return offset < 0 ? null : data[offset];
    }

    public T add(T value) {
        int offset = Arrays.binarySearch(data, 0, size, value);
        if (offset < 0) {
            int idx = -(offset + 1);
            if (size == capacity) {
                capacity += GROWTH;
                data = Arrays.copyOf(data, capacity);
            }
            if (idx != size) {
                System.arraycopy(data, idx, data, idx + 1, size - idx);
            }
            size++;
            data[idx] = value;
            return value;
        } else {
            return data[offset];
        }
    }

    public int getCapacity() {
        return capacity;
    }

    public int getSize() {
        return size;
    }

    public T[] items() {
        return Arrays.copyOf(data, size);
    }
}
