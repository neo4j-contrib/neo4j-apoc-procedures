package org.HdrHistogram;

/**
 * @author mh
 * @since 20.12.17
 */
public class HistogramUtil {
    public static DoubleHistogram toDoubleHistogram(Histogram source, int numberOfSignificantValueDigits) {
        DoubleHistogram doubles = new DoubleHistogram(numberOfSignificantValueDigits);
        // Do max value first, to avoid max value updates on each iteration:
        int otherMaxIndex = source.countsArrayIndex(source.getMaxValue());
        long count = source.getCountAtIndex(otherMaxIndex);
        doubles.recordValueWithCount(source.valueFromIndex(otherMaxIndex), count);

        // Record the remaining values, up to but not including the max value:
        for (int i = 0; i < otherMaxIndex; i++) {
            count = source.getCountAtIndex(i);
            if (count > 0) {
                doubles.recordValueWithCount(source.valueFromIndex(i), count);
            }
        }
        return doubles;
    }
}
