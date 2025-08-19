package apoc.neo4j.docker;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import org.junit.*;
import org.neo4j.driver.Session;
import org.neo4j.driver.summary.ResultSummary;

import java.util.List;
import java.util.Map;

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testResult;

public class ImportExportEnterpriseTest {
    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() throws InterruptedException {
        neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.EXTENDED, TestContainerUtil.ApocPackage.CORE), true)
                .withNeo4jConfig("apoc.import.file.enabled", "true")
                .withNeo4jConfig("apoc.export.file.enabled", "true")
                .withNeo4jConfig("internal.cypher.enable_vector_type", "true");
        neo4jContainer.start();
        session = neo4jContainer.getSession();

        

    }
    
    @AfterClass
    public static void afterAll() {
        neo4jContainer.close();
    }
    
    @Before
    public void before() {
        var vectorTypes = List.of("INT64", "INT32", "INT16", "INT8", "FLOAT64", "FLOAT32");
        for (String type : vectorTypes) {
            session.executeWrite(
                    tx -> tx.run("CYPHER 25 CREATE (:VectorFoo { z: VECTOR([1, 2, 3], 3, %s) });".formatted(type)).consume()
            );
        }
    }  
    
    @After
    public void after() {
        session.executeWrite(tx -> tx.run("MATCH (n:VectorFoo) DETACH DELETE n").consume());
    }

    @Test
    public void testParquet() {
        ResultSummary resultSummary = session.executeWrite(tx -> tx.run("CALL apoc.export.parquet.all('test.parquet')").consume());
        System.out.println("resultSummary = " + resultSummary);

        ResultSummary resultSummary1 = session.executeWrite(tx -> tx.run("CALL apoc.import.parquet('test.parquet')").consume());
        System.out.println("resultSummary1 = " + resultSummary1);

        ResultSummary resultSummary2 = session.executeWrite(tx -> tx.run("CALL apoc.load.parquet('test.parquet')").consume());
        System.out.println("resultSummary2 = " + resultSummary2);

        testResult(session, "CALL apoc.load.parquet('test.parquet')", r -> {
            Map<String, Object> next = r.next();
            System.out.println("next = " + next);
            
        });
        
        // todo - wait for next driver versions?
        // --- session.run("MATCH (n) RETURN n").list()
        /*
            org.neo4j.driver.internal.util.ErrorUtil$InternalExceptionCause
            detailMessage = "Struct tag: 0x56 representing type VECTOR is not supported for this protocol version"
            cause = {Neo4jException@7483} "org.neo4j.driver.exceptions.Neo4jException: 22NBD: Unsupported struct tag: 0x56."
         */

        System.out.println("resultSummary2 = " + resultSummary2);
    }

    @Test
    public void testXls() {
        // TODO 
    }

    @Test
    public void testArrow() {
        // TODO - import and export, since load that is in LoadArrowExtended
    }

    @Test
    public void testCsv() {
        // TODO - test just the load, if the export is feasible, otherwise write a new issue or so
    }
}
