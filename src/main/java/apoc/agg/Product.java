package apoc.agg;

import org.HdrHistogram.Histogram;
import org.neo4j.procedure.*;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mh
 * @since 18.12.17
 */
public class Product {
    @UserAggregationFunction("apoc.agg.product")
    @Description("apoc.agg.product(number) - returns given product for non-null values")
    public ProductFunction product() {
        return new ProductFunction();
    }


    public static class ProductFunction {

        private double doubleProduct = 1.0D;
        private long longProduct = 1L;
        private int count = 0;

        @UserAggregationUpdate
        public void aggregate(@Name("number") Number number) {
            if (number != null) {
                if (number instanceof Long) {
                    longProduct = Math.multiplyExact(longProduct,number.longValue());
                } else if (number instanceof Double) {
                    doubleProduct *= number.doubleValue();
                }
                count++;
            }
        }

        @UserAggregationResult
        public Number result() {
            if (count == 0) return 0D;
            if (longProduct == 1L) return doubleProduct;
            if (doubleProduct == 1.0) return longProduct;
            return doubleProduct*longProduct;
        }
    }
}
