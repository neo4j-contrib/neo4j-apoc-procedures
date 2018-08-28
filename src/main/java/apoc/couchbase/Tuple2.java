package apoc.couchbase;

/**
 * Created by alberto.delazzari on 23/08/2018.
 */
public final class Tuple2<T1, T2> {

    private final T1 v1;

    private final T2 v2;

    Tuple2(final T1 v1, final T2 v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    public T1 v1() {
        return v1;
    }

    public T2 v2() {
        return v2;
    }
}