package apoc.load.jdbc;

import apoc.periodic.Periodic;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.stream.IntStream;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testResult;

/**
 * Query times with 4 million nodes:
 * - table creation: 25 ms
 * - table population: 68438 ms
 * - return data from table: 958 ms
 * - TOTAL: 69421 ms
 */
@Ignore("This test check DuckDB analytics performances, we ignore it since it's slow and just log the times spent")
public class DuckDBJdbcPerformanceTest extends AbstractJdbcTest {

    public String JDBC_DUCKDB = null;
    
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    private Connection conn;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        JDBC_DUCKDB = "jdbc:duckdb:" + temporaryFolder.newFolder() + "/testDB";
        apocConfig().setProperty("apoc.jdbc.duckdb.url", JDBC_DUCKDB);
        apocConfig().setProperty("apoc.jdbc.test.sql","SELECT * FROM PERSON");
        apocConfig().setProperty("apoc.jdbc.testparams.sql","SELECT * FROM PERSON WHERE NAME = ?");
        TestUtil.registerProcedure(db, Jdbc.class, Periodic.class, Analytics.class);
        
        conn = DriverManager.getConnection(JDBC_DUCKDB);

        IntStream.range(0, 200)
                .forEach(__-> db.executeTransactionally("UNWIND range(0, 19999) as id WITH id CREATE (:City {country: 'country' + id, name: 'name' + id, year: id * 2, population: id})"));

        Map<String, Object> stringObjectMap = db.executeTransactionally("match (n) return count(n)", Map.of(), Result::next);
        System.out.println("stringObjectMap = " + stringObjectMap);
    }

    @After
    public void tearDown() throws SQLException {
        conn.close();
    }

    @Test
    public void testLoadJdbcAnalytics() {
        String cypher = "MATCH (n:City) RETURN n.country AS country, n.name AS name, n.year AS year, n.population AS population";

        String sql = """
            SELECT
                country,
                name,
                year,
                population,
                RANK() OVER (PARTITION BY country ORDER BY year DESC) AS rank
            FROM %s
            ORDER BY rank, country, name;
            """
            .formatted(Analytics.TABLE_NAME_DEFAULT_CONF_KEY);
        
        long startTime = System.currentTimeMillis();
        testResult(db, "CALL apoc.jdbc.analytics($queryCypher, $url, $sql) YIELD row RETURN count(*)",
                map(
                        "queryCypher", cypher,
                        "sql", sql,
                        "url", JDBC_DUCKDB
                ),
                Result::resultAsString);
        
        long totalTime = System.currentTimeMillis() - startTime;
        System.out.println("Total time: " + totalTime);
    }

}
