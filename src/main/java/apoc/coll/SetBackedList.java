package apoc.coll;

import java.util.*;

/**
 * @author mh
 * @since 10.04.16
 */
class SetBackedList extends AbstractSequentialList {

    private final Set set;

    public SetBackedList(Set set) {
        this.set = set;
    }

    @Override
    public int size() {
        return set.size();
    }

    public ListIterator listIterator(int index) {
        return new ListIterator() {
            Iterator it = set.iterator();
            Object current = null;
            int idx = 0;
            {
                moveTo(index);
            }

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public Object next() {
                idx++;
                return current = it.next();
            }

            @Override
            public boolean hasPrevious() {
                return idx > 0;
            }

            @Override
            public Object previous() {
                if (!hasPrevious()) throw new NoSuchElementException();
                Object tmp = current;
                moveTo(idx-1);
                return tmp;
            }

            private void moveTo(int pos) {
                Iterator it2 = set.iterator();
                Object value = null;
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
}
