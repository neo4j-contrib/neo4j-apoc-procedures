package apoc.date;

import java.util.concurrent.TimeUnit;

public class DateUtils {
	public static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static TimeUnit unit(String unit) {
        if (unit == null) return TimeUnit.MILLISECONDS;

        switch (unit.toLowerCase()) {
            case "ms": case "milli":  case "millis": case "milliseconds": return TimeUnit.MILLISECONDS;
            case "s":  case "second": case "seconds": return TimeUnit.SECONDS;
            case "m":  case "minute": case "minutes": return TimeUnit.MINUTES;
            case "h":  case "hour":   case "hours":   return TimeUnit.HOURS;
            case "d":  case "day":    case "days":    return TimeUnit.DAYS;
//			case "month":case "months": return TimeUnit.MONTHS;
//			case "years":case "year": return TimeUnit.YEARS;
        }

        throw new IllegalArgumentException("The unit: "+ unit + " is not correct");

        //return TimeUnit.MILLISECONDS;
    }
}
