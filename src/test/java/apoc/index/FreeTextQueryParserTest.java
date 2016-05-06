package apoc.index;

import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import static apoc.index.FreeTextQueryParser.parseFreeTextQuery;
import static org.apache.lucene.search.NumericRangeQuery.newDoubleRange;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class FreeTextQueryParserTest {
    @Test
    public void shouldParseNumericRange() throws Exception {
        assertNumRange("num:[10 TO 15]", 10.0, 15.0);
        assertNumRange("num:[10 TO *]", 10.0, null);
        assertNumRange("num:[* TO 20]", null, 20.0);
        assertTextRange("num:[* TO *]", null, null);
        assertTextRange("[10 TO 15]", "10", "15");
        assertTextRange("num:[1 TO two]", "1", "two");
        assertTextRange("num:[one TO 2]", "one", "2");
    }

    private static void assertNumRange(String query, Double lo, Double hi) throws ParseException {
        Query parsed = parseFreeTextQuery(query);
        assertThat(parsed, CoreMatchers.instanceOf(NumericRangeQuery.class));
        assertEquals(newDoubleRange(((NumericRangeQuery) parsed).getField(), lo, hi, true, true), parsed);
    }

    private static void assertTextRange(String query, String lo, String hi) throws ParseException {
        Query parsed = parseFreeTextQuery(query);
        assertThat(parsed, CoreMatchers.instanceOf(TermRangeQuery.class));
        assertEquals(new TermRangeQuery(((TermRangeQuery) parsed).getField(),
                lo == null ? null : new BytesRef(lo),
                hi == null ? null : new BytesRef(hi), true, true), parsed);
    }
}
