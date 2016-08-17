package apoc.index;


import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import static apoc.index.FreeTextSearch.KEY;
import static org.apache.lucene.search.NumericRangeQuery.newDoubleRange;

class FreeTextQueryParser extends QueryParser {

    private static char[] WILDCARDS = new char[]{'?','*'};

    static Query parseFreeTextQuery(String query) throws ParseException {

        boolean useWildCardParser = false;
        for (int i=0; i<WILDCARDS.length; i++) {
            if (query.charAt(0)==WILDCARDS[i]) {
                useWildCardParser=true;
                break;
            }
        }

        ThreadLocal<QueryParser> parser = useWildCardParser ? PARSER_ALLOWING_LEADING_WILDCARDS : PARSER;
        return parser.get().parse(query);
    }

    private static final ThreadLocal<QueryParser> PARSER = new ThreadLocal<QueryParser>() {
        @Override
        protected QueryParser initialValue() {
            return new FreeTextQueryParser(false);
        }
    };

    private static final ThreadLocal<QueryParser> PARSER_ALLOWING_LEADING_WILDCARDS = new ThreadLocal<QueryParser>() {
        @Override
        protected QueryParser initialValue() {
            return new FreeTextQueryParser(true);
        }
    };

    private FreeTextQueryParser(boolean allowLeadingWildcard) {
        super(KEY, FreeTextSearch.analyzer());
        setAllowLeadingWildcard(allowLeadingWildcard);
    }

    @Override
    protected Query newRangeQuery(String field, String part1, String part2, boolean startInclusive, boolean endInclusive) {
        if (!KEY.equals(field)) {
            Double num1 = parseNumber(part1), num2 = parseNumber(part2);
            if ((num1 != null || num2 != null) && !(num1 == null && part1 != null) && !(num2 == null && part2 != null)) {
                return newDoubleRange(field, num1, num2, startInclusive, endInclusive);
            }
        }
        return super.newRangeQuery(field, part1, part2, startInclusive, endInclusive);
    }

    private static Double parseNumber(String number) {
        try {
            return number == null ? null : Double.parseDouble(number);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
