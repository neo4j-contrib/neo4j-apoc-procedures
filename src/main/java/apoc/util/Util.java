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
/*
    @Procedure
    public Stream<ListResult> sortNodes(@Name("coll") List coll, @Name("prop") String prop) {
        List sorted = new ArrayList(coll);
        Collections.sort((List<? extends PropertyContainer>)sorted, (x,y) -> x.getProperty(prop, null) y.getProperty(prop, null) );
        return Stream.of(new ListResult(sorted));
    }
*/
}