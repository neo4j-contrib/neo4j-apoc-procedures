package apoc.coll;

import apoc.Extended;
import com.google.common.util.concurrent.AtomicDouble;
import org.apache.commons.collections4.CollectionUtils;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;
import org.neo4j.values.storable.DurationValue;

import java.time.temporal.ChronoUnit;
import java.util.List;

@Extended
public class CollFull {

    @UserFunction
    @Description("apoc.coll.avgDuration([duration('P2DT3H'), duration('PT1H45S'), ...]) -  returns the average of a list of duration values")
    public DurationValue avgDuration(@Name("numbers") List<DurationValue> list) {
        if (CollectionUtils.isEmpty(list)) return null;
        long count = list.size();
        final AtomicDouble monthsRunningAvg = new AtomicDouble();
        final AtomicDouble daysRunningAvg = new AtomicDouble();
        final AtomicDouble secondsRunningAvg = new AtomicDouble();
        final AtomicDouble nanosRunningAvg = new AtomicDouble();
        for (DurationValue duration : list) {
            monthsRunningAvg.addAndGet(duration.get(ChronoUnit.MONTHS));
            daysRunningAvg.addAndGet(duration.get(ChronoUnit.DAYS));
            secondsRunningAvg.addAndGet(duration.get(ChronoUnit.SECONDS));
            nanosRunningAvg.addAndGet(duration.get(ChronoUnit.NANOS));
        }

        return DurationValue.approximate(
                monthsRunningAvg.get() / count,
                daysRunningAvg.get() / count,
                secondsRunningAvg.get() / count,
                nanosRunningAvg.get() / count
        ).normalize();
    }
}
