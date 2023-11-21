package apoc.util;

import static apoc.export.cypher.formatter.CypherFormatterUtils.formatProperties;
import static apoc.export.cypher.formatter.CypherFormatterUtils.formatToString;
import static apoc.util.Util.getAllQueryProcs;

import java.math.BigInteger;
import java.time.Duration;
import java.time.temporal.TemporalAccessor;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import apoc.util.collection.Iterators;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.Result;
import org.neo4j.procedure.Mode;

public class ExtendedUtil
{
    public static String dateFormat( TemporalAccessor value, String format){
        return Util.getFormat(format).format(value);
    }

    public static double doubleValue( Entity pc, String prop, Number defaultValue) {
        return Util.toDouble(pc.getProperty(prop, defaultValue));
    }

    public static Duration durationParse(String value) {
        return Duration.parse(value);
    }

    public static boolean isSumOutOfRange(long... numbers) {
        try {
            sumLongs(numbers).longValueExact();
            return false;
        } catch (ArithmeticException ae) {
            return true;
        }
    }

    private static BigInteger sumLongs(long... numbers) {
        return LongStream.of(numbers)
                .mapToObj(BigInteger::valueOf)
                .reduce(BigInteger.ZERO, (x, y) -> x.add(y));
    }

    public static String toCypherMap( Map<String, Object> map) {
        final StringBuilder builder = formatProperties(map);
        return "{" + formatToString(builder) + "}";
    }

    // Similar to `boolean isQueryTypeValid` located in Util.java (APOC Core)
    public static QueryExecutionType.QueryType isQueryValid(Result result, QueryExecutionType.QueryType[] supportedQueryTypes) {
        QueryExecutionType.QueryType type = result.getQueryExecutionType().queryType();
        // if everything is ok return null, otherwise the current getQueryExecutionType().queryType()
        if (supportedQueryTypes != null && supportedQueryTypes.length != 0 && Stream.of(supportedQueryTypes).noneMatch(sqt -> sqt.equals(type))) {
            return type;
        }
        return null;
    }

    public static boolean procsAreValid(GraphDatabaseService db, Set<Mode> supportedModes, Result result) {
        if (supportedModes != null && !supportedModes.isEmpty()) {
            final ExecutionPlanDescription executionPlanDescription = result.getExecutionPlanDescription();
            // get procedures used in the query
            Set<String> queryProcNames = new HashSet<>();
            getAllQueryProcs(executionPlanDescription, queryProcNames);

            if (!queryProcNames.isEmpty()) {
                final Set<String> modes = supportedModes.stream().map(Mode::name).collect(Collectors.toSet());
                // check if sub-procedures have valid mode
                final Set<String> procNames = db.executeTransactionally("SHOW PROCEDURES YIELD name, mode where mode in $modes return name",
                        Map.of("modes", modes),
                        r -> Iterators.asSet(r.columnAs("name")));

                return procNames.containsAll(queryProcNames);
            }
        }

        return true;
    }
}
