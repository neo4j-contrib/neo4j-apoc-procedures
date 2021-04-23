package apoc.load;

import apoc.util.TestUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static apoc.load.LoadHtml.KEY_ERROR;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
public class LoadHtmlTestWithSelenium {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setup() {
        TestUtil.registerProcedure(db, LoadHtml.class);
    }

    @Test
    public void testQueryAllTODO() {
        // TODO - CI METTO UNO SCRIPT INTERNO ED UNO ESTERNO...

        Map<String, Object> query = map("td", "td");

        testResult(db, "CALL apoc.load.html($url,$query,$config)", map("url",new File("src/test/resources/testhtml1.zip!wikipedia.html").toURI().toString(), "query", query,
                "config", map("withGeneratedJs", "FIREFOX")),
                result -> {
                    Map<String, Object> row = result.next();
                    Map<String, Object> value = (Map<String, Object>) row.get("value");

                    List<Map<String, Object>> td = (List<Map<String, Object>>) value.get("td");
                    List<Map<String, Object>> td2 = (List<Map<String, Object>>) value.get("td");
//                    List<Map<String, Object>> h2 = (List<Map<String, Object>>) value.get("h2");

                    assertEquals(asList("RESULT_QUERY_METADATA").toString().trim(), td.toString().trim());
//                    assertEquals(asList(RESULT_QUERY_H2).toString().trim(), h2.toString().trim());
                });
    }

    // TODO - TEST CON BASEURL

    // TODO - TEST CHE SPACCA SE CONFIG ERRATA

}
