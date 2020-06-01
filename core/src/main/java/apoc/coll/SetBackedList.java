package apoc.coll;

import java.util.*;

/**
 * @author mh
 * @since 10.04.16
 */
public class SetBackedList<T> extends AbstractSequentialList<T> implements Set<T> {

    private final Set<T> set;

    public SetBackedList(Set<T> set) {
        this.set = set;
    }

    @Override
    public int size() {
        return set.size();
    }

    public ListIterator<T> listIterator(int index) {
        return new ListIterator<T>() {
            Iterator<T> it = set.iterator();
            T current = null;
            int idx = 0;
            {
                moveTo(index);
            }

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public T next() {
                idx++;
                return current = it.next();
            }

            @Override
            public boolean hasPrevious() {
                return idx > 0;
            }

            @Override
            public T previous() {
                if (!hasPrevious()) throw new NoSuchElementException();
                T tmp = current;
                moveTo(idx-1);
                return tmp;
            }

            private void moveTo(int pos) {
                Iterator<T> it2 = set.iterator();
                T value = null;
                int i=0; while (i++<pos) { value = it2.next(); };
                this.it = it2;
                this.idx = pos;
                this.current = value;
            }

            @Override
            public int nextIndex() {
                return idx;
            }

            @Override
            public int previousIndex() {
                return idx - 1;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove");
            }

            @Override
            public void set(Object o) {
                throw new UnsupportedOperationException("set");
            }

            @Override
            public void add(Object o) {
                throw new UnsupportedOperationException("add");
            }
        };
    }

    @Override
    public boolean contains(Object o) {
        return set.contains(o);
    }

    @Override
    public int hashCode() {
        return set.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Set) return set.equals(o);
        return o instanceof Iterable && super.equals(o);
    }

    @Override
    public Spliterator<T> spliterator() {
        return set.spliterator();
    }
}
