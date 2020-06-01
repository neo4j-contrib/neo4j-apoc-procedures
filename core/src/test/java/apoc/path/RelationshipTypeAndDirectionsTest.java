package apoc.path;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Direction.*;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.helpers.collection.Iterables.iterable;

@RunWith(Parameterized.class)
public class RelationshipTypeAndDirectionsTest {

    @Parameters(name = "{index}: {0} -> {1}")
    public static Iterable<Object[]> data() {
          return Arrays.asList(new Object[][] {
                  {null, iterable(Pair.of(null, BOTH))},
                  {"", iterable(Pair.of(null, BOTH))},
                  {">", iterable(Pair.of(null, OUTGOING))},
                  {"<", iterable(Pair.of(null, INCOMING))},
                  {"SIMPLE", iterable(Pair.of(withName("SIMPLE"), BOTH))},
                  {"SIMPLE>", iterable(Pair.of(withName("SIMPLE"), OUTGOING))},
                  {"SIMPLE<", iterable(Pair.of(withName("SIMPLE"), INCOMING))},
                  {"<SIMPLE", iterable(Pair.of(withName("SIMPLE"), INCOMING))},
                  {">SIMPLE", iterable(Pair.of(withName("SIMPLE"), OUTGOING))},
                  {"SIMPLE", iterable(Pair.of(withName("SIMPLE"), BOTH))},
                  {"TYPE1|TYPE2", iterable(
                          Pair.of(withName("TYPE1"), BOTH),
                          Pair.of(withName("TYPE2"), BOTH))
                  },
                  {"TYPE1>|TYPE2<", iterable(
                          Pair.of(withName("TYPE1"), OUTGOING),
                          Pair.of(withName("TYPE2"), INCOMING))
                  },
             });
      }

    @Parameter(value = 0)
    public String input;

    @Parameter(value = 1)
    public Iterable<Pair<RelationshipType, Direction>> expected;

    @Test
    public void parse() throws Exception {
        assertEquals(expected, RelationshipTypeAndDirections.parse(input));
    }

}
