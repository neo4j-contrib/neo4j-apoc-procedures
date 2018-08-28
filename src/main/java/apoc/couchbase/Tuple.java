package apoc.couchbase;

/**
 * Created by alberto.delazzari on 23/08/2018.
 */
public class Tuple {

    private Tuple() {
    }

    public static <T1, T2> Tuple2<T1, T2> create(T1 v1, T2 v2) {
        return new Tuple2<>(v1, v2);
    }
}
