package apoc.util;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.neo4j.graphdb.QueryExecutionException;

public class StatusCodeMatcher extends BaseMatcher<QueryExecutionException> {

    private final String statusCode;

    public StatusCodeMatcher(String statusCode) {
        this.statusCode = statusCode;
    }

    @Override
    public boolean matches(Object item) {
        return statusCode.equals(((QueryExecutionException)item).getStatusCode());
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("statuscode does not match: ").appendValue(statusCode);
    }

}
