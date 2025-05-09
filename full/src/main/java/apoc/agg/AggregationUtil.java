package apoc.agg;

import java.util.Map;

public class AggregationUtil {

    public static void updateAggregationValues(
            Map<String, Number> partialResult, Object property, String countKey, String sumKey, String avgKey) {
        Number count = updateCountValue(partialResult, countKey);

        updateSumAndAvgValues(partialResult, property, count.doubleValue(), sumKey, avgKey);
    }

    private static Number updateCountValue(Map<String, Number> partialResult, String countKey) {
        Number count = partialResult.compute(countKey, ((subKey, subVal) -> {
            return subVal == null ? 1 : subVal.longValue() + 1;
        }));
        return count;
    }

    private static void updateSumAndAvgValues(
            Map<String, Number> partialResult, Object property, double count, String sumKey, String avgKey) {
        if (!(property instanceof Number)) {
            return;
        }

        Number numberProp = (Number) property;

        Number sum = partialResult.compute(sumKey, ((subKey, subVal) -> {
            if (subVal == null) {
                if (numberProp instanceof Long) {
                    return numberProp;
                }
                return numberProp.doubleValue();
            }
            if (subVal instanceof Long && numberProp instanceof Long) {
                Long long2 = (Long) numberProp;
                Long long1 = (Long) subVal;
                return long1 + long2;
            }
            return subVal.doubleValue() + numberProp.doubleValue();
        }));

        partialResult.compute(avgKey, ((subKey, subVal) -> {
            if (subVal == null) {
                return numberProp.doubleValue();
            }
            return sum.doubleValue() / count;
        }));
    }
}
