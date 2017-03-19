package apoc.coll;

import org.neo4j.kernel.impl.util.statistics.IntCounter;
import org.neo4j.procedure.*;
import apoc.result.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
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

    @UserFunction
    @Description("apoc.coll.zip([list1],[list2])")
    public List<List<Object>> zip(@Name("list1") List<Object> list1, @Name("list2") List<Object> list2) {
        List<List<Object>> result = new ArrayList<>(list1.size());
        ListIterator it = list2.listIterator();
        for (Object o1 : list1) {
            result.add(asList(o1,it.hasNext() ? it.next() : null));
        }
        return result;
    }

    @UserFunction
    @Description("apoc.coll.pairs([1,2,3]) returns [1,2],[2,3],[3,null] ")
    public List<List<Object>> pairs(@Name("list") List<Object> list) {
        return zip(list,list.subList(1,list.size()));
    }
    @UserFunction
    @Description("apoc.coll.pairsMin([1,2,3]) returns [1,2],[2,3]")
    public List<List<Object>> pairsMin(@Name("list") List<Object> list) {
        return zip(list.subList(0,list.size()-1),list.subList(1,list.size()));
    }

    @UserFunction
    @Description("apoc.coll.sum([0.5,1,2.3])")
    public double sum(@Name("numbers") List<Number> list) {
        double sum = 0;
        for (Number number : list) {
            sum += number.doubleValue();
        }
        return sum;
    }

    @UserFunction
    @Description("apoc.coll.avg([0.5,1,2.3])")
    public double avg(@Name("numbers") List<Number> list) {
        double avg = 0;
        for (Number number : list) {
            avg += number.doubleValue();
        }
        return (avg/(double)list.size());
    }
    @UserFunction
    @Description("apoc.coll.min([0.5,1,2.3])")
    public Object min(@Name("values") List<Object> list) {
        return Collections.min((List)list);
    }

    @UserFunction
    @Description("apoc.coll.max([0.5,1,2.3])")
    public Object max(@Name("values") List<Object> list) {
        return Collections.max((List)list);
    }

    @Procedure
    @Description("apoc.coll.partition(list,batchSize)")
    public Stream<ListResult> partition(@Name("values") List<Object> list, @Name("batchSize") long batchSize) {
        return partitionList(list, (int) batchSize).map(ListResult::new);
    }

    @Procedure
    @Description("apoc.coll.split(list,value) | splits collection on given values rows of lists, value itself will not be part of resulting lists")
    public Stream<ListResult> split(@Name("values") List<Object> list, @Name("value") Object value) {
        List<Object> l = new ArrayList<>(list);
        List<List<Object>> result = new ArrayList<>(10);
        int idx = l.indexOf(value);
        while (idx != -1) {
            List<Object> subList = l.subList(0, idx);
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

    @UserFunction
    @Description("apoc.coll.contains(coll, value) optimized contains operation (using a HashSet) (returns single row or not)")
    public boolean contains(@Name("coll") List<Object> coll, @Name("value") Object value) {
        return  new HashSet<>(coll).contains(value);
//        int batchSize = 250;
//        boolean result = (coll.size() < batchSize) ? coll.contains(value) : partitionList(coll, batchSize).parallel().anyMatch(list -> list.contains(value));
    }

    @UserFunction
    @Description("apoc.coll.indexOf(coll, value) | position of value in the list")
    public long indexOf(@Name("coll") List<Object> coll, @Name("value") Object value) {
        return  new ArrayList<>(coll).indexOf(value);
    }

    @UserFunction
    @Description("apoc.coll.containsAll(coll, values) optimized contains-all operation (using a HashSet) (returns single row or not)")
    public boolean containsAll(@Name("coll") List<Object> coll, @Name("values") List<Object> values) {
        return new HashSet<>(coll).containsAll(values);
    }

    @UserFunction
    @Description("apoc.coll.containsSorted(coll, value) optimized contains on a sorted list operation (Collections.binarySearch) (returns single row or not)")
    public boolean containsSorted(@Name("coll") List<Object> coll, @Name("value") Object value) {
        int batchSize = 5000-1; // Collections.binarySearchThreshold
        List list = (coll instanceof RandomAccess || coll.size() < batchSize) ? coll : new ArrayList(coll);
        return Collections.binarySearch(list, value) >= 0;
//        Predicate<List> contains = l -> Collections.binarySearch(l, value) >= 0;
//        boolean result = (list.size() < batchSize) ? contains.test(list) : partitionList(list, batchSize).parallel().anyMatch(contains);
    }

    @UserFunction
    @Description("apoc.coll.containsAllSorted(coll, value) optimized contains-all on a sorted list operation (Collections.binarySearch) (returns single row or not)")
    public boolean containsAllSorted(@Name("coll") List<Object> coll, @Name("values") List<Object> values) {
        int batchSize = 5000-1; // Collections.binarySearchThreshold
        List list = (coll instanceof RandomAccess || coll.size() < batchSize) ? coll : new ArrayList(coll);
        for (Object value : values) {
            boolean result = Collections.binarySearch(list, value) >= 0;
            if (!result) return false;
        }
        return true;
    }


    @UserFunction
    @Description("apoc.coll.toSet([list]) returns a unique list backed by a set")
    public List<Object> toSet(@Name("values") List<Object> list) {
        return new SetBackedList(new LinkedHashSet(list));
    }

    @UserFunction
    @Description("apoc.coll.sumLongs([1,3,3])")
    public long sumLongs(@Name("numbers") List<Number> list) {
        long sum = 0;
        for (Number number : list) {
            sum += number.longValue();
        }
        return sum;
    }

    @UserFunction
    @Description("apoc.coll.sort(coll) sort on Collections")
    public List<Object> sort(@Name("coll") List<Object> coll) {
        List sorted = new ArrayList<>(coll);
        Collections.sort((List<? extends Comparable>) sorted);
        return sorted;
    }

    @UserFunction
    @Description("apoc.coll.sortNodes([nodes], 'name') sort nodes by property")
    public List<Node> sortNodes(@Name("coll") List<Node> coll, @Name("prop") String prop) {
        List<Node> sorted = new ArrayList<>(coll);
        Collections.sort(sorted,
                (x, y) -> compare(x.getProperty(prop, null), y.getProperty(prop, null)));
        return sorted;
    }

    @UserFunction
    @Description("apoc.coll.sortMaps([maps], 'name') - sort maps by property")
    public List<Map<String,Object>> sortMaps(@Name("coll") List<Map<String,Object>> coll, @Name("prop") String prop) {
        List<Map<String,Object>> sorted = new ArrayList<>(coll);
        sorted.sort((x, y) -> compare(x.get(prop), y.get(prop)));
        return sorted;
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

    @UserFunction
    @Description("apoc.coll.union(first, second) - creates the distinct union of the 2 lists")
    public List<Object> union(@Name("first") List<Object> first, @Name("second") List<Object> second) {
        Set<Object> set = new HashSet<>(first);
        set.addAll(second);
        return new SetBackedList(set);
    }
    @UserFunction
    @Description("apoc.coll.subtract(first, second) - returns unique set of first list with all elements of second list removed")
    public List<Object> subtract(@Name("first") List<Object> first, @Name("second") List<Object> second) {
        Set<Object> set = new HashSet<>(first);
        set.removeAll(second);
        return new SetBackedList(set);
    }
    @UserFunction
    @Description("apoc.coll.removeAll(first, second) - returns first list with all elements of second list removed")
    public List<Object> removeAll(@Name("first") List<Object> first, @Name("second") List<Object> second) {
        List<Object> list = new ArrayList<>(first);
        list.removeAll(second);
        return list;
    }

    @UserFunction
    @Description("apoc.coll.intersection(first, second) - returns the unique intersection of the two lists")
    public List<Object> intersection(@Name("first") List<Object> first, @Name("second") List<Object> second) {
        Set<Object> set = new HashSet<>(first);
        set.retainAll(second);
        return new SetBackedList(set);
    }

    @UserFunction
    @Description("apoc.coll.disjunction(first, second) - returns the disjunct set of the two lists")
    public List<Object> disjunction(@Name("first") List<Object> first, @Name("second") List<Object> second) {
        Set<Object> intersection = new HashSet<>(first);
        intersection.retainAll(second);
        Set<Object> set = new HashSet<>(first);
        set.addAll(second);
        set.removeAll(intersection);
        return new SetBackedList(set);
    }
    @UserFunction
    @Description("apoc.coll.unionAll(first, second) - creates the full union with duplicates of the two lists")
    public List<Object> unionAll(@Name("first") List<Object> first, @Name("second") List<Object> second) {
        List<Object> list = new ArrayList<>(first);
        list.addAll(second);
        return list;
    }

    @UserFunction
    @Description("apoc.coll.shuffle(coll) - returns the shuffled list")
    public List<Object> shuffle(@Name("coll") List<Object> coll) {
        if (coll == null || coll.isEmpty()) {
            return Collections.emptyList();
        } else if (coll.size() == 1) {
            return coll;
        }

        List<Object> shuffledList = new ArrayList<>(coll);
        Collections.shuffle(shuffledList);
        return shuffledList;
    }

    @UserFunction
    @Description("apoc.coll.randomItem(coll)- returns a random item from the list, or null on an empty or null list")
    public Object randomItem(@Name("coll") List<Object> coll) {
        if (coll == null || coll.isEmpty()) {
            return null;
        } else if (coll.size() == 1) {
            return coll.get(0);
        }

        return coll.get(ThreadLocalRandom.current().nextInt(coll.size()));
    }

    @UserFunction
    @Description("apoc.coll.randomItems(coll, itemCount, allowRepick: false) - returns a list of itemCount random items from the original list, optionally allowing picked elements to be picked again")
    public List<Object> randomItems(@Name("coll") List<Object> coll, @Name("itemCount") long itemCount, @Name(value = "allowRepick", defaultValue = "false") boolean allowRepick) {
        if (coll == null || coll.isEmpty() || itemCount <= 0) {
            return Collections.emptyList();
        }

        List<Object> pickList = new ArrayList<>(coll);
        List<Object> randomItems = new ArrayList<>((int)itemCount);
        Random random = ThreadLocalRandom.current();

        if (!allowRepick && itemCount >= coll.size()) {
            Collections.shuffle(pickList);
            return pickList;
        }

        while (randomItems.size() < itemCount) {
            Object item = allowRepick ? pickList.get(random.nextInt(pickList.size()))
                    : pickList.remove(random.nextInt(pickList.size()));
            randomItems.add(item);
        }

        return randomItems;
    }
    @UserFunction
    @Description("apoc.coll.containsDuplicates(coll) - returns true if a collection contains duplicate elements")
    public boolean containsDuplicates(@Name("coll") List<Object> coll) {
        if (coll == null || coll.size() <= 1) {
            return false;
        }

        Set<Object> set = new HashSet<>(coll);
        return set.size() < coll.size();
    }

    @UserFunction
    @Description("apoc.coll.duplicates(coll) - returns a list of duplicate items in the collection")
    public List<Object> duplicates(@Name("coll") List<Object> coll) {
        if (coll == null || coll.size() <= 1) {
            return Collections.emptyList();
        }

        Set<Object> set = new HashSet<>(coll.size());
        Set<Object> duplicates = new LinkedHashSet<>();

        for (Object obj : coll) {
            if (!set.add(obj)) {
                duplicates.add(obj);
            }
        }

        return new ArrayList(duplicates);
    }

    @UserFunction
    @Description("apoc.coll.duplicatesWithCount(coll) - returns a list of duplicate items in the collection and their count, keyed by `item` and `count` (e.g., `[{item: xyz, count:2}, {item:zyx, count:5}]`)")
    public List<Map<String, Object>> duplicatesWithCount(@Name("coll") List<Object> coll) {
        if (coll == null || coll.size() <= 1) {
            return Collections.emptyList();
        }

        // mimicking a counted bag
        Map<Object, IntCounter> duplicates = new LinkedHashMap<>(coll.size());
        List<Map<String, Object>> resultList = new ArrayList<>();

        for (Object obj : coll) {
            IntCounter counter = duplicates.get(obj);
            if (counter == null) {
                counter = new IntCounter();
                duplicates.put(obj, counter);
            }
            counter.increment();
        }

        duplicates.forEach((o, intCounter) -> {
            int count = intCounter.value();
            if (count > 1) {
                Map<String, Object> entry = new LinkedHashMap<>(2);
                entry.put("item", o);
                entry.put("count", Long.valueOf(count));
                resultList.add(entry);
            }
        });

        return resultList;
    }

    @UserFunction
    @Description("apoc.coll.occurrences(coll, item) - returns the count of the given item in the collection")
    public long occurrences(@Name("coll") List<Object> coll, @Name("item") Object item) {
        if (coll == null || coll.isEmpty()) {
            return 0;
        }

        long occurrences = 0;

        for (Object obj : coll) {
            if (item.equals(obj)) {
                occurrences++;
            }
        }

        return occurrences;
    }

    @UserFunction
    @Description("apoc.coll.reverse(coll) - returns reversed list")
    public List<Object> reverse(@Name("coll") List<Object> coll) {
        if (coll == null || coll.isEmpty()) {
            return Collections.emptyList();
        } else if (coll.size() == 1) {
            return coll;
        }

        List<Object> reversed = new ArrayList<Object>(coll);
        Collections.reverse(reversed);

        return reversed;
    }
}
