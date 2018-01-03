package apoc.data;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.Collection;
import java.util.List;

public class Aggregate {

    @UserFunction
    @Description("apoc.data.first(collection) | get first value from given collection")
    public Object first(final @Name("collection") List collection) {
        if (isInvalid(collection, 1)) {
            return null;
        }
        return collection.get(0);
    }

    @UserFunction
    @Description("apoc.data.last(collection) | get last value from given collection")
    public Object last(final @Name("collection") List collection) {
        if (isInvalid(collection, collection.size())) {
            return null;
        }
        return collection.get(collection.size() - 1);
    }

    @UserFunction
    @Description("apoc.data.nth(collection, skip) | get n-th element from collection")
    public Object nth(final @Name("collection") List collection, final @Name("skip") long skip) {
        if (isInvalid(collection, (int) skip) || skip < 0) {
            return null;
        }
        return collection.get((int) skip);
    }

    @UserFunction
    @Description("apoc.data.subset(collection, skip, limit) | get subset of a collection")
    public Object subset(final @Name("collection") List collection, final @Name("skip") long skip, final @Name("limit") long limit) {
        if (isInvalid(collection, (int)(skip + limit)) || skip < 0 || limit < 0) {
            return null;
        }
        return collection.subList((int) skip, (int)(skip + limit));
    }

    private boolean isInvalid(Collection collection, int requiredSize) {
        return collection == null || collection.size() < requiredSize;
    }
}
