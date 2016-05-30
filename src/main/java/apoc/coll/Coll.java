package apoc.coll;

import apoc.Description;
import apoc.result.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Arrays.asList;

public class Coll {

    @Context
    public GraphDatabaseService db;

    @Procedure
    @Description("apoc.coll.zipToRows(list1,list2) - creates pairs like zip but emits one row per pair")
    public Stream<ListResult> zipToRows(@Name("list1") List<Object> list1, @Name("list2") List<Object> list2) {
        if (list1.isEmpty()) return Stream.empty();
        ListIterator<Object> it = list2.listIterator();
        return list1.stream().map((e) -> new ListResult(asList(e,it.hasNext() ? it.next() : null)));
    }

    @Procedure
    @Description("apoc.coll.zip([list1],[list2])")
    public Stream<ListResult> zip(@Name("list1") List<Object> list1, @Name("list2") List<Object> list2) {
        List<List<Object>> result = new ArrayList<>(list1.size());
        ListIterator<Object> it = list2.listIterator();
        for (Object o1 : list1) {
            result.add(asList(o1,it.hasNext() ? it.next() : null));
        }
        return Stream.of(new ListResult(result));
    }
    @Procedure
    @Description("apoc.coll.pairs([list]) returns [first,second],[second,third], ...")
    public Stream<ListResult> pairs(@Name("list") List<Object> list) {
        return zip(list,list.subList(1,list.size()));
    }

    @Procedure
    @Description("apoc.coll.sum([0.5,1,2.3])")
    public Stream<DoubleResult> sum(@Name("numbers") List<Number> list) {
        double sum = 0;
        for (Number number : list) {
            sum += number.doubleValue();
        }
        return Stream.of(new DoubleResult(sum));
    }

    @Procedure
    @Description("apoc.coll.avg([0.5,1,2.3])")
    public Stream<DoubleResult> avg(@Name("numbers") List<Number> list) {
        double avg = 0;
        for (Number number : list) {
            avg += number.doubleValue();
        }
        return Stream.of(new DoubleResult(avg/(double)list.size()));
    }
    @Procedure
    @Description("apoc.coll.min([0.5,1,2.3])")
    public Stream<ObjectResult> min(@Name("values") List<Object> list) {
        return Stream.of(new ObjectResult(Collections.min((List)list)));
    }

    @Procedure
    @Description("apoc.coll.max([0.5,1,2.3])")
    public Stream<ObjectResult> max(@Name("values") List<Object> list) {
        return Stream.of(new ObjectResult(Collections.max((List)list)));
    }

    @Procedure
    @Description("apoc.coll.partition(list,batchSize)")
    public Stream<ListResult> partition(@Name("values") List<Object> list, @Name("batchSize") long batchSize) {
        return partitionList(list, (int) batchSize).map(ListResult::new);
    }
    @Procedure
    @Description("apoc.coll.split(list,value) | splits collection on given values rows of lists, value itself will not be part of resulting lists")
    public Stream<ListResult> split(@Name("values") List<Object> list, @Name("value") Object value) {
        List l = new ArrayList<>(list);
        List<List> result = new ArrayList<>(10);
        int idx = l.indexOf(value);
        while (idx != -1) {
            List subList = l.subList(0, idx);
            if (!subList.isEmpty()) result.add(subList);
            l = l.subList(idx+1,l.size());
            idx = l.indexOf(value);
        }
        if (!l.isEmpty()) result.add(l);
        return result.stream().map(ListResult::new);
    }

    private Stream<List<Object>> partitionList(@Name("values") List list, @Name("batchSize") int batchSize) {
        int total = list.size();
        int pages = (total / batchSize) + 1;
        return IntStream.range(0, pages).parallel().boxed()
                .map(page -> {
                    int from = page * batchSize;
                    return list.subList(from, Math.min(from + batchSize, total));
                });
    }

    @Procedure
    @Description("apoc.coll.contains(coll, value) optimized contains operation (using a HashSet) (returns single row or not)")
    public Stream<Empty> contains(@Name("coll") List<Object> coll, @Name("value") Object value) {
        boolean result =  new HashSet<>(coll).contains(value);
//        int batchSize = 250;
//        boolean result = (coll.size() < batchSize) ? coll.contains(value) : partitionList(coll, batchSize).parallel().anyMatch(list -> list.contains(value));
        return Empty.stream(result);
    }

    @Procedure
    @Description("apoc.coll.indexOf(coll, value) | position of value in the list")
    public Stream<LongResult> indexOf(@Name("coll") List<Object> coll, @Name("value") Object value) {
        int result =  new ArrayList<>(coll).indexOf(value);
        return Stream.of(new LongResult((long) result));
    }

    @Procedure
    @Description("apoc.coll.containsAll(coll, values) optimized contains-all operation (using a HashSet) (returns single row or not)")
    public Stream<Empty> containsAll(@Name("coll") List<Object> coll, @Name("values") List<Object> values) {
        boolean result =  new HashSet<>(coll).containsAll(values);
        return Empty.stream(result);
    }

    @Procedure
    @Description("apoc.coll.containsSorted(coll, value) optimized contains on a sorted list operation (Collections.binarySearch) (returns single row or not)")
    public Stream<Empty> containsSorted(@Name("coll") List<Object> coll, @Name("value") Object value) {
        int batchSize = 5000-1; // Collections.binarySearchThreshold
        List list = (coll instanceof RandomAccess || coll.size() < batchSize) ? coll : new ArrayList(coll);
        boolean result = Collections.binarySearch(list, value) >= 0;
//        Predicate<List> contains = l -> Collections.binarySearch(l, value) >= 0;
//        boolean result = (list.size() < batchSize) ? contains.test(list) : partitionList(list, batchSize).parallel().anyMatch(contains);
        return Empty.stream(result);
    }

    @Procedure
    @Description("apoc.coll.containsAllSorted(coll, value) optimized contains-all on a sorted list operation (Collections.binarySearch) (returns single row or not)")
    public Stream<Empty> containsAllSorted(@Name("coll") List<Object> coll, @Name("values") List<Object> values) {
        int batchSize = 5000-1; // Collections.binarySearchThreshold
        List list = (coll instanceof RandomAccess || coll.size() < batchSize) ? coll : new ArrayList(coll);
        for (Object value : values) {
            boolean result = Collections.binarySearch(list, value) >= 0;
            if (!result) return Stream.empty();
        }
        return Empty.stream(true);
    }


    @Procedure
    @Description("apoc.coll.toSet([list]) returns a unique list backed by a set")
    public Stream<ListResult> toSet(@Name("values") List<Object> list) {
        return Stream.of(new ListResult(new SetBackedList(new LinkedHashSet(list))));
    }

    @Procedure
    @Description("apoc.coll.sumLongs([1,3,3])")
    public Stream<LongResult> sumLongs(@Name("numbers") List<Number> list) {
        long sum = 0;
        for (Number number : list) {
            sum += number.longValue();
        }
        return Stream.of(new LongResult(sum));
    }

    @Procedure
    @Description("apoc.coll.sort(coll) sort on Collections")
    public Stream<ListResult> sort(@Name("coll") List coll) {
        List sorted = new ArrayList(coll);
        Collections.sort((List<? extends Comparable>) sorted);
        return Stream.of(new ListResult(sorted));
    }

    @Procedure
    @Description("apoc.coll.sortNodes([nodes], 'name') sort nodes by property")
    public Stream<ListResult> sortNodes(@Name("coll") List coll, @Name("prop") String prop) {
        List sorted = new ArrayList(coll);
        Collections.sort((List<? extends PropertyContainer>) sorted,
                (x, y) -> compare(x.getProperty(prop, null), y.getProperty(prop, null)));
        return Stream.of(new ListResult(sorted));
    }

    public static int compare(Object o1, Object o2) {
        if (o1 == null) return o2 == null ? 0 : -1;
        if (o2 == null) return 1;
        if (o1.equals(o2)) return 0;
        if (o1 instanceof Number && o2 instanceof Number) {
            if (o1 instanceof Double || o2 instanceof Double || o1 instanceof Float || o2 instanceof Float)
                return Double.compare(((Number) o1).doubleValue(), ((Number) o2).doubleValue());
            return Long.compare(((Number) o1).longValue(), ((Number) o2).longValue());
        }
        if (o1 instanceof Boolean && o2 instanceof Boolean) return ((Boolean) o1) ? 1 : -1;
        if (o1 instanceof Node && o2 instanceof Node) return Long.compare(((Node)o1).getId(),((Node)o2).getId());
        if (o1 instanceof Relationship && o2 instanceof Relationship) return Long.compare(((Relationship)o1).getId(),((Relationship)o2).getId());
        return o1.toString().compareTo(o2.toString());
    }

    @Procedure
    @Description("apoc.coll.union(first, second) - creates the distinct union of the 2 lists")
    public Stream<ListResult> union(@Name("first") List<Object> first, @Name("second") List<Object> second) {
        Set<Object> set = new HashSet<>(first);
        set.addAll(second);
        return Stream.of(new ListResult(new SetBackedList(set)));
    }
    @Procedure
    @Description("apoc.coll.subtract(first, second) - returns unique set of first list with all elements of second list removed")
    public Stream<ListResult> subtract(@Name("first") List<Object> first, @Name("second") List<Object> second) {
        Set<Object> set = new HashSet<>(first);
        set.removeAll(second);
        return Stream.of(new ListResult(new SetBackedList(set)));
    }
    @Procedure
    @Description("apoc.coll.removeAll(first, second) - returns first list with all elements of second list removed")
    public Stream<ListResult> removeAll(@Name("first") List<Object> first, @Name("second") List<Object> second) {
        List<Object> list = new ArrayList<>(first);
        list.removeAll(second);
        return Stream.of(new ListResult(list));
    }

    @Procedure
    @Description("apoc.coll.intersection(first, second) - returns the unique intersection of the two lists")
    public Stream<ListResult> intersection(@Name("first") List<Object> first, @Name("second") List<Object> second) {
        Set<Object> set = new HashSet<>(first);
        set.retainAll(second);
        return Stream.of(new ListResult(new SetBackedList(set)));
    }

    @Procedure
    @Description("apoc.coll.disjunction(first, second) - returns the disjunct set of the two lists")
    public Stream<ListResult> disjunction(@Name("first") List<Object> first, @Name("second") List<Object> second) {
        Set<Object> intersection = new HashSet<>(first);
        intersection.retainAll(second);
        Set<Object> set = new HashSet<>(first);
        set.addAll(second);
        set.removeAll(intersection);
        return Stream.of(new ListResult(new SetBackedList(set)));
    }
    @Procedure
    @Description("apoc.coll.unionAll(first, second) - creates the full union with duplicates of the two lists")
    public Stream<ListResult> unionAll(@Name("first") List<Object> first, @Name("second") List<Object> second) {
        List<Object> list = new ArrayList<>(first);
        list.addAll(second);
        return Stream.of(new ListResult(list));
    }

}
