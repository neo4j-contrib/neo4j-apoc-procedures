package apoc.util;

import java.util.*;
import java.util.stream.*;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;
import org.neo4j.logging.Log;

public class Util {

    @Context public org.neo4j.graphdb.GraphDatabaseService db;

    public static BooleanResult TRUE = new BooleanResult(true);
    public static BooleanResult FALSE = new BooleanResult(false);
    public static class BooleanResult {
        public final boolean value;
        public BooleanResult(boolean value) {this.value = value;}
    }

    public static class ListResult {
        public final List value;
        public ListResult(List value) {this.value = value;}
    }

    @Procedure
    // not needed here @PerformsWrites
    public Stream<BooleanResult> IN(@Name("value") Object value, @Name("coll") List<Object> coll) {
        return Stream.of(new HashSet(coll).contains(value) ? TRUE : FALSE);
    }

    @Procedure
    public Stream<ListResult> sort(@Name("coll") List coll) {
        List sorted = new ArrayList(coll);
        Collections.sort((List<? extends Comparable>)sorted);
        return Stream.of(new ListResult(sorted));
    }

    @Procedure
    public Stream<ListResult> sortNodes(@Name("coll") List coll, @Name("prop") String prop) {
        List sorted = new ArrayList(coll);
        Collections.sort((List<? extends PropertyContainer>)sorted, 
                         (x,y) -> compare(x.getProperty(prop, null), y.getProperty(prop, null)));
        return Stream.of(new ListResult(sorted));
    }
    public static int compare(Object o1, Object o2) {
	    if (o1 == null) return -1;
	    if (o1 == null) return o2 == null ? 0 : -1;
	    if (o2 == null) return 1;
	    if (o1.equals(o2)) return 0;
        if (o1 instanceof Number && o2 instanceof Number) {
	        if (o1 instanceof Double || o2 instanceof Double  || o1 instanceof Float|| o2 instanceof Float) 
				return Double.compare(((Number)o1).doubleValue(),((Number)o2).doubleValue());
			return Long.compare(((Number)o1).longValue(),((Number)o2).longValue());
		}
        if (o1 instanceof Boolean && o2 instanceof Boolean) return ((Boolean)o1) ? 1 : -1;
        return o1.toString().compareTo(o2.toString());
    }
}