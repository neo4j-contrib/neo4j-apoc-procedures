package apoc.coll;

import apoc.util.ArrayBackedIterator;
import apoc.util.ArrayBackedList;
import org.junit.Test;
import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.lang.reflect.Array;
import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Arrays.hashCode;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author mh
 * @since 02.06.16
 */
public class EqualityTest {

    @Test
    public void testSpecial() throws Exception {
        shouldNotMatch((byte) 23, 23.5);
    }

    @Test
    public void testEquality() {
        shouldMatch(1.0, 1L);
        shouldMatch(1.0, 1);
        shouldMatch(1.0, 1.0);
        shouldMatch(0.9, 0.9);
        shouldMatch(Math.PI, Math.PI);
        shouldMatch(1.1, 1.1);
        shouldMatch(0, 0);
        shouldMatch(Double.NaN, Double.NaN);
        shouldMatch((double) Integer.MAX_VALUE, Integer.MAX_VALUE);
        shouldMatch((double) Long.MAX_VALUE, Long.MAX_VALUE);
        shouldMatch(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
        shouldMatch(Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
    }

    @Test
    public void testPropertyEquality() throws Exception {
        shouldMatch(true, true);
        shouldMatch(false, false);
        shouldNotMatch(true, false);
        shouldNotMatch(false, true);
        shouldNotMatch(true, 0);
        shouldNotMatch(false, 0);
        shouldNotMatch(true, 1);
        shouldNotMatch(false, 1);
        shouldNotMatch(false, "false");
        shouldNotMatch(true, "true");

        //byte properties
        shouldMatch((byte) 42, (byte) 42);
        shouldMatch((byte) 42, (short) 42);
        shouldNotMatch((byte) 42, 42 + 256);
        shouldMatch((byte) 43, (int) 43);
        shouldMatch((byte) 43, (long) 43);
        shouldMatch((byte) 23, 23.0d);
        shouldMatch((byte) 23, 23.0f);
        shouldNotMatch((byte) 23, 23.5);
        shouldNotMatch((byte) 23, 23.5f);

        //short properties
        shouldMatch((short) 11, (byte) 11);
        shouldMatch((short) 42, (short) 42);
        shouldNotMatch((short) 42, 42 + 65536);
        shouldMatch((short) 43, (int) 43);
        shouldMatch((short) 43, (long) 43);
        shouldMatch((short) 23, 23.0f);
        shouldMatch((short) 23, 23.0d);
        shouldNotMatch((short) 23, 23.5);
        shouldNotMatch((short) 23, 23.5f);

        //int properties
        shouldMatch(11, (byte) 11);
        shouldMatch(42, (short) 42);
        shouldNotMatch(42, 42 + 4294967296L);
        shouldMatch(43, 43);
        shouldMatch(Integer.MAX_VALUE, Integer.MAX_VALUE);
        shouldMatch(43, (long) 43);
        shouldMatch(23, 23.0);
        shouldNotMatch(23, 23.5);
        shouldNotMatch(23, 23.5f);

        //long properties
        shouldMatch(11L, (byte) 11);
        shouldMatch(42L, (short) 42);
        shouldMatch(43L, (int) 43);
        shouldMatch(43L, (long) 43);
        shouldMatch(87L, (long) 87);
        shouldMatch(Long.MAX_VALUE, Long.MAX_VALUE);
        shouldMatch(23L, 23.0);
        shouldNotMatch(23L, 23.5);
        shouldNotMatch(23L, 23.5f);
        shouldMatch(9007199254740992L, 9007199254740992D);
        // shouldMatch(9007199254740993L, 9007199254740992D); // is stupid, m'kay?!

        // floats goddamnit
        shouldMatch(11f, (byte) 11);
        shouldMatch(42f, (short) 42);
        shouldMatch(43f, (int) 43);
        shouldMatch(43f, (long) 43);
        shouldMatch(23f, 23.0);
        shouldNotMatch(23f, 23.5);
        shouldNotMatch(23f, 23.5f);
        shouldMatch(3.14f, 3.14f);
        shouldMatch(3.14f, 3.14d);   // Would be nice if they matched, but they don't

        // doubles
        shouldMatch(11d, (byte) 11);
        shouldMatch(42d, (short) 42);
        shouldMatch(43d, (int) 43);
        shouldMatch(43d, (long) 43);
        shouldMatch(23d, 23.0);
        shouldNotMatch(23d, 23.5);
        shouldNotMatch(23d, 23.5f);
        shouldMatch(3.14d, 3.14f);   // this really is sheeeet
        shouldMatch(3.14d, 3.14d);


        // strings
        shouldMatch("A", "A");
        shouldMatch('A', 'A');
        shouldMatch('A', "A");
        shouldMatch("A", 'A');
        shouldNotMatch("AA", 'A');
        shouldNotMatch("a", "A");
        shouldNotMatch("A", "a");
        shouldNotMatch("0", 0);
        shouldNotMatch('0', 0);

        // arrays
        shouldMatch(new int[]{1, 2, 3}, new int[]{1, 2, 3});
        shouldMatch(new int[][]{{1}, {2,2}, {3,3,3}}, new double[][]{{1.0}, {2.0,2.0}, {3.0,3.0,3.0}});
        shouldMatch(new int[]{1, 2, 3}, new long[]{1, 2, 3});
        shouldMatch(new int[]{1, 2, 3}, new double[]{1.0, 2.0, 3.0});
        shouldMatch(new String[]{"A", "B", "C"}, new String[]{"A", "B", "C"});
        shouldMatch(new String[]{"A", "B", "C"}, new char[]{'A', 'B', 'C'});
        shouldMatch(new char[]{'A', 'B', 'C'}, new String[]{"A", "B", "C"});

        // collections
        shouldMatch(new int[]{1, 2, 3}, asList(1, 2, 3));
        shouldMatch(asList(1, 2, 3), asList(1L, 2L, 3L));
        shouldMatch(new int[]{1, 2, 3}, asList(1L, 2L, 3L));
        shouldMatch(new int[]{1, 2, 3}, asList(1.0D, 2.0D, 3.0D));
        shouldMatch(new Object[]{1, new int[]{2,2}, 3}, asList(1.0D, asList(2.0D,2.0D), 3.0D));
        shouldMatch(new String[]{"A", "B", "C"}, asList("A", "B", "C"));
        shouldMatch(new String[]{"A", "B", "C"}, asList('A', 'B', 'C'));
        shouldMatch(new char[]{'A', 'B', 'C'}, asList("A", "B", "C"));
    }

    @Test
    public void testGraphEntities() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        try (Transaction tx = db.beginTx()) {
            Node n1 = db.createNode();
            Node n2 = db.createNode();
            Relationship r1 = n1.createRelationshipTo(n2, RelationshipType.withName("TEST"));
            Relationship r2 = n2.createRelationshipTo(n1, RelationshipType.withName("TEST"));

            shouldMatch(n1,n1);
            shouldMatch(asList(n1,n2),asList(n1,n2));
            shouldMatch(asList(n1,n2),new Node[]{n1,n2});
            shouldNotMatch(n1,n2);
            shouldNotMatch(n2,n1);
            shouldNotMatch(asList(n2),asList(n1));
            shouldMatch(r1,r1);
            shouldMatch(asList(r1,r2),asList(r1,r2));
            shouldMatch(asList(r1,r2),new Relationship[]{r1,r2});
            shouldNotMatch(r1,r2);
            shouldNotMatch(r2,r1);
            shouldNotMatch(asList(r2),asList(r1));
            Path p1 = new PathImpl.Builder(n1).push(r1).build();
            Path p2 = new PathImpl.Builder(n2).push(r2).build();
            shouldMatch(p1,p1);
            shouldMatch(asList(p1,p2),asList(p1,p2));
            shouldMatch(asList(p1,p2),new Path[]{p1,p2});
            shouldNotMatch(p1,p2);
            shouldNotMatch(p2,p1);
            shouldNotMatch(asList(p2),asList(p1));
        }
        db.shutdown();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testIn() throws Exception {
        Set<Object> set = new HashSet(asList(0, 1, 2.0D, 3.0F, 4L, 'A', "B", new int[]{0, 1, 2, 3}, asList(0, 1L, 2F, 3D), true, null)) {
            @Override
            public boolean add(Object o) {
                return super.add(new Same(o));
            }
            @Override
            public boolean contains(Object o) {
                return super.contains(new Same(o));
            }
        };
        assertTrue(set.contains(true));
        assertTrue(set.contains(null));
        assertTrue(set.contains(1));
        assertTrue(set.contains(1D));
        assertTrue(set.contains(1.0D));
        assertTrue(set.contains(1F));
        assertTrue(set.contains(1L));
        assertTrue(set.contains(3));
        assertTrue(set.contains(3D));
        assertTrue(set.contains(3.0D));
        assertTrue(set.contains(3F));
        assertTrue(set.contains('A'));
        assertTrue(set.contains("A"));
        assertTrue(set.contains('B'));
        assertTrue(set.contains("B"));
        assertTrue(set.contains(new int[]{0,1,2,3}));
        assertTrue(set.contains(new double[]{0,1,2,3}));
        assertTrue(set.contains(new long[]{0,1,2,3}));
        assertTrue(set.contains(new Object[]{0,1.0,2L,3.0D}));
        assertTrue(set.contains(asList(0,1.0,2L,3.0D)));
    }

    private void shouldMatch(Object v1, Object v2) {
        assertTrue(Same.equals(v1, v2));
    }

    private void shouldNotMatch(Object v1, Object v2) {
        assertFalse(Same.equals(v1, v2));
    }

    private static class Same {
        private final int hashCode;
        private final Object o;

        public Same(Object o) {
            this.o = o;
            this.hashCode = hashCode(o);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Same) {
                return equals(o, ((Same)obj).o);
            }
            return equals(o,obj);
        }

        public static boolean equals(Object o1, Object o2) {
            if (o1 == null || o2 == null) return o1 == o2;
            if (o1 instanceof Number && o2 instanceof Number) return equals((Number) o1, (Number) o2);
            if (o1 instanceof String) return equals((String) o1, o2);
            if (o2 instanceof String) return equals((String) o2, o1);
            if (o1.getClass().isArray()) return equals(new ArrayBackedList(o1), o2);
            if (o2.getClass().isArray()) return equals(new ArrayBackedList(o2), o1);
            if (o1 instanceof Collection) return equals((Collection) o1, o2); // todo iterable?
            if (o2 instanceof Collection) return equals((Collection) o2, o1); // todo iterable?
            return o1.equals(o2);
        }

        public static int hashCode(Object o) {
            if (o == null) return 0;
            if (o instanceof Number) return ((Number)o).intValue();
            if (o.getClass().isArray()) {
                int length = Array.getLength(o);
                return length > 0 ? length * (hashCode(Array.get(o,0)) + hashCode(Array.get(o,length - 1))) : 42;
            }
            if (o instanceof List) {
                List list = (List) o;
                int length = list.size();
                return length > 0 ? length * (hashCode(list.get(0)) + hashCode(list.get(length - 1))) : 42;
            }
            if (o instanceof Iterable) {
                Iterator it = ((Iterable) o).iterator();
                return it.hasNext() ? hashCode(it.next()): 123;
            }
            return o.hashCode();
        }

        public static boolean equals(Collection c1, Object c2) {
            if (c2.getClass().isArray()) c2 = new ArrayBackedIterator(c2);
//        if (c2 instanceof Collection) return c1.equals(c2);
            if (c2 instanceof Iterable) c2 = ((Iterable) c2).iterator();
            if (c2 instanceof Iterator) {
                Iterator it1 = c1.iterator();
                Iterator it2 = (Iterator) c2;
                while (it2.hasNext() && it1.hasNext()) {
                    if (!equals(it1.next(), it2.next())) return false;
                }
                return it1.hasNext() == it2.hasNext();
            }
            return false;
        }

        public static boolean equals(String s1, Object s2) {
            if (s2 instanceof CharSequence) return s1.equals(String.valueOf(s2));
            if (s2 instanceof Character) return s1.equals(String.valueOf((char) s2));
            return false;
        }

        public static boolean equals(Number v1, Number v2) {
            return v1.doubleValue() == v2.doubleValue() ||
                    ((long) Math.rint(v1.doubleValue()) == v2.longValue()
                            && v1.longValue() == (long) Math.rint(v2.doubleValue()));
        }

    }

}
