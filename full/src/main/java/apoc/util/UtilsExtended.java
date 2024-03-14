package apoc.util;

import apoc.Extended;
import org.neo4j.procedure.*;

@Extended
public class UtilsExtended {

    @UserFunction("apoc.util.hashCode")
    @Description("apoc.util.hashCode(value) - Returns the java.lang.Object#hashCode() of the value")
    public long hashCode(@Name("value") Object value) {
        return value.hashCode();
    }
}
