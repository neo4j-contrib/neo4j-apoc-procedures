package apoc.util;

import static apoc.export.cypher.formatter.CypherFormatterUtils.formatProperties;
import static apoc.export.cypher.formatter.CypherFormatterUtils.formatToString;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URI;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.temporal.TemporalAccessor;
import java.util.Map;
import java.util.stream.LongStream;
import org.neo4j.graphdb.Entity;

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

    public static boolean isLocalAddress(String url) {
        try {
            String host = URI.create(url).getHost();
                    
            InetAddress address = InetAddress.getByName(host);
            return address.isAnyLocalAddress()
                   || address.isLoopbackAddress()
                   || NetworkInterface.getByInetAddress(address) != null;
        } catch (UnknownHostException | SocketException e) {
            // ignored
        }
        return false;
    }
}
