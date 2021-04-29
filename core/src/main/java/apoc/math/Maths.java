package apoc.math;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @author mh
 * @since 12.12.16
 */
public class Maths {
    @UserFunction(deprecatedBy = "Neo4j round() function. This function will be removed in version 5.0")
    @Deprecated
    @Description("apoc.math.round(value,[prec],mode=[CEILING,FLOOR,UP,DOWN,HALF_EVEN,HALF_DOWN,HALF_UP,DOWN,UNNECESSARY])")
    public Double round(@Name("value") Double value,
                        @Name(value = "precision",defaultValue = "0") long precision,
                        @Name(value = "mode",defaultValue = "HALF_UP") String mode) {
        if (value == null) return null;
        return BigDecimal.valueOf(value).setScale((int)precision, RoundingMode.valueOf(mode)).doubleValue();
    }

    @UserFunction
    @Description("apoc.math.maxLong() | return the maximum value a long can have")
    public Long maxLong(){
        return Long.MAX_VALUE;
    }

    @UserFunction
    @Description("apoc.math.minLong() | return the minimum value a long can have")
    public Long minLong(){
        return Long.MIN_VALUE;
    }

    @UserFunction
    @Description("apoc.math.maxDouble() | return the largest positive finite value of type double")
    public Double maxDouble(){
        return Double.MAX_VALUE;
    }

    @UserFunction
    @Description("apoc.math.minDouble() | return the smallest positive nonzero value of type double")
    public Double minDouble(){
        return Double.MIN_VALUE;
    }

    @UserFunction
    @Description("apoc.math.maxInt() | return the maximum value an int can have")
    public Long maxInt(){
        return Long.valueOf(Integer.MAX_VALUE);
    }

    @UserFunction
    @Description("apoc.math.minInt() | return the minimum value an int can have")
    public Long minInt(){
        return Long.valueOf(Integer.MIN_VALUE);
    }

    @UserFunction
    @Description("apoc.math.maxByte() | return the maximum value an byte can have")
    public Long maxByte(){
        return Long.valueOf(Byte.MAX_VALUE);
    }

    @UserFunction
    @Description("apoc.math.minByte() | return the minimum value an byte can have")
    public Long minByte(){
        return Long.valueOf(Byte.MIN_VALUE);
    }


}
