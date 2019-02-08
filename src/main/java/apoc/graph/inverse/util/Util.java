package apoc.graph.inverse.util;

public class Util {

    private Object value;

    public Util(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("value cannot be null");
        }
        this.value = value;
    }

    public String toCypher() {
        if (isString()) {
            return "'" + toString().replaceAll("'", "\\\\'") + "'";
        }
        return toString();
    }

    public boolean isString() {
        return (value instanceof String);
    }

    @Override
    public String toString() {
        return value.toString();
    }

}
