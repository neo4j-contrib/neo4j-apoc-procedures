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
    @UserFunction
    @Description("apoc.math.round(value,[prec],mode=[CEILING,FLOOR,UP,DOWN,HALF_EVEN,HALF_DOWN,HALF_UP,DOWN,UNNECESSARY])")
    public Double round(@Name("value") Double value,
                        @Name(value = "precision",defaultValue = "0") long precision,
                        @Name(value = "mode",defaultValue = "HALF_UP") String mode) {
        if (value == null) return null;
        return BigDecimal.valueOf(value).setScale((int)precision, RoundingMode.valueOf(mode)).doubleValue();
    }
}
