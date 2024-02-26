package apoc.agg;

import apoc.Extended;
import apoc.util.collection.Iterables;
import apoc.util.collection.Iterators;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;

import java.util.Map;
import java.util.function.BiPredicate;

@Extended
public class AggregationExtended {

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @UserAggregationFunction("apoc.agg.row")
    @Description("apoc.agg.row(element, predicate) - Returns index of the `element` that match the given `predicate`")
    public RowFunction row() {
        BiPredicate<Object, Object> curr =  (current, value) -> db.executeTransactionally("RETURN " + value,
                Map.of("curr", current),
                result -> Iterators.singleOrNull(result.<Boolean>columnAs(Iterables.single(result.columns()))));
        return new RowFunction(curr);
    }
    
    @UserAggregationFunction("apoc.agg.position")
    @Description("apoc.agg.position(element, value) - Returns index of the `element` that match the given `value`")
    public RowFunction position() {
        return new RowFunction(Object::equals);
    }

    public static class RowFunction {
        private boolean found;
        private final BiPredicate<Object, Object> biPredicate;
        private long index = -1L;

        public RowFunction(BiPredicate<Object, Object> biPredicate) {
            this.biPredicate = biPredicate;
        }

        @UserAggregationUpdate
        public void update(@Name("value") Object value, @Name("element") Object element) {
            if (!found) {
                try {
                    found = this.biPredicate.test(value, element);
                } catch (Exception e) {
                    throw new RuntimeException("The predicate query has thrown the following exception: \n" + e.getMessage());
                }
                index++;
            }
        }

        @UserAggregationResult
        public Object result() {
            return index;
        }
    }
}
