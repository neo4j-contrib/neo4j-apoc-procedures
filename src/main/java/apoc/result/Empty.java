package apoc.result;

import apoc.meta.Meta;

import java.util.stream.Stream;

/**
 * @author mh
 * @since 09.04.16
 */
public class Empty {
    private static final Empty INSTANCE = new Empty();

    public static Stream<Empty> stream(boolean value) { return value ? Stream.of(INSTANCE) : Stream.empty(); }
}
