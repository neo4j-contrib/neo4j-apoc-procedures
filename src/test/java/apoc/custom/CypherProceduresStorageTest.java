package apoc.custom;

import apoc.util.JsonUtil;
import apoc.util.MapUtil;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.core.GraphPropertiesProxy;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author mh
 * @since 18.08.18
 */
public class CypherProceduresStorageTest {

    public static final File STORE_DIR = new File("cypher-storage");
    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new GraphDatabaseFactory().newEmbeddedDatabase(STORE_DIR);
        TestUtil.registerProcedure(db, CypherProcedures.class);
    }

    @After
    public void tearDown() throws IOException {
        db.shutdown();
        FileUtils.deleteRecursively(STORE_DIR);
    }

    private void restartDb() throws KernelException {
        db.shutdown();
        db = new GraphDatabaseFactory().newEmbeddedDatabase(STORE_DIR);
        TestUtil.registerProcedure(db, CypherProcedures.class);
    }
    @Test
    public void registerSimpleStatement() throws Exception {
        db.execute("call apoc.custom.asProcedure('answer','RETURN 42 as answer')");
        restartDb();
        TestUtil.testCall(db, "call custom.answer()", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
    }

    @Test
    public void registerSimpleStatementConcreteResults() throws Exception {
        db.execute("call apoc.custom.asProcedure('answer','RETURN 42 as answer','read',[['answer','long']])");
        restartDb();
        TestUtil.testCall(db, "call custom.answer()", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerParameterStatement() throws Exception {
        db.execute("call apoc.custom.asProcedure('answer','RETURN $answer as answer')");
        restartDb();
        TestUtil.testCall(db, "call custom.answer({answer:42})", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
    }

    @Test
    public void registerConcreteParameterStatement() throws Exception {
        db.execute("call apoc.custom.asProcedure('answer','RETURN $input as answer','read',null,[['input','number']])");
        restartDb();
        TestUtil.testCall(db, "call custom.answer(42)", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
    }

    @Test
    public void registerConcreteParameterAndReturnStatement() throws Exception {
        db.execute("call apoc.custom.asProcedure('answer','RETURN $input as answer','read',[['answer','number']],[['input','int','42']])");
        restartDb();
        TestUtil.testCall(db, "call custom.answer()", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void testAllParameterTypes() throws Exception {
        db.execute("call apoc.custom.asProcedure('answer','RETURN [$int,$float,$string,$map,$`list int`,$bool,$date,$datetime,$point] as data','read',null," +
                "[['int','int'],['float','float'],['string','string'],['map','map'],['list int','list int'],['bool','bool'],['date','date'],['datetime','datetime'],['point','point']])");
        restartDb();
        TestUtil.testCall(db, "call custom.answer(42,3.14,'foo',{a:1},[1],true,date(),datetime(),point({x:1,y:2}))", (row) -> assertEquals(9, ((List)((Map)row.get("row")).get("data")).size()));
    }

    @Test
    public void registerSimpleStatementFunction() throws Exception {
        db.execute("call apoc.custom.asFunction('answer','RETURN 42 as answer')");
        restartDb();
        TestUtil.testCall(db, "return custom.answer() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
    }

    @Test
    public void registerSimpleStatementConcreteResultsFunction() throws Exception {
        db.execute("call apoc.custom.asFunction('answer','RETURN 42 as answer','long')");
        restartDb();
        TestUtil.testCall(db, "return custom.answer() as answer", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerSimpleStatementConcreteResultsFunctionUnnamedResultColumn() throws Exception {
        db.execute("call apoc.custom.asFunction('answer','RETURN 42','long')");
        restartDb();
        TestUtil.testCall(db, "return custom.answer() as answer", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerParameterStatementFunction() throws Exception {
        db.execute("call apoc.custom.asFunction('answer','RETURN $answer as answer','long')");
        restartDb();
        TestUtil.testCall(db, "return custom.answer({answer:42}) as answer", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerConcreteParameterAndReturnStatementFunction() throws Exception {
        db.execute("call apoc.custom.asFunction('answer','RETURN $input as answer','long',[['input','number']])");
        restartDb();
        TestUtil.testCall(db, "return custom.answer(42) as answer", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void testAllParameterTypesFunction() throws Exception {
        db.execute("call apoc.custom.asFunction('answer','RETURN [$int,$float,$string,$map,$`list int`,$bool,$date,$datetime,$point] as data','list of any'," +
                "[['int','int'],['float','float'],['string','string'],['map','map'],['list int','list int'],['bool','bool'],['date','date'],['datetime','datetime'],['point','point']], true)");
        restartDb();
        TestUtil.testCall(db, "return custom.answer(42,3.14,'foo',{a:1},[1],true,date(),datetime(),point({x:1,y:2})) as data", (row) -> assertEquals(9, ((List)row.get("data")).size()));
    }

    @Test
    public void handleFunctionsWithNoneDescriptions() throws Exception {
        // Anyone that knows the right knobs can store whatever they like in the graph properties store;
        // hence we should make sure we handle the case of someone having modified our JSON blob
        JobScheduler fakeScheduler = Mockito.mock(JobScheduler.class);
        Log fakeLog = Mockito.mock(Log.class);
        GraphDatabaseAPI gds = (GraphDatabaseAPI) this.db;

        // Given I've stored a function with no description..
        CypherProcedures.CustomProcedureStorage.storeFunction(gds, new UserFunctionSignature( new QualifiedName(new String[]{"apoc"}, "nodescription"),
                Collections.emptyList(), Neo4jTypes.NTBoolean, "", new String[]{}, null, false), "my query", false);

        // When I load the stored state..
        CypherProcedures.CustomProcedureStorage store = new CypherProcedures.CustomProcedureStorage(fakeScheduler, gds, fakeLog);
        store.available();

        // Then its ok
        List<CypherProcedures.CustomProcedureInfo> result = store.list();
        assertEquals(1, result.size());
        CypherProcedures.CustomProcedureInfo myFuncInfo = result.get(0);
        assertNull(myFuncInfo.description);
    }

    @Test
    public void handlePersistedStateWithMapAsFunctionDescription() throws Exception {
        // Anyone that knows the right knobs can store whatever they like in the graph properties store;
        // hence we should make sure we handle the case of someone having modified our JSON blob
        JobScheduler fakeScheduler = Mockito.mock(JobScheduler.class);
        Log fakeLog = Mockito.mock(Log.class);
        GraphDatabaseAPI gds = (GraphDatabaseAPI) this.db;

        // Given the database contains an old state blob with a map in the `description` field of a function..
        UserFunctionSignature signature = new UserFunctionSignature(new QualifiedName(new String[]{"apoc"}, "nodescription"),
                Collections.emptyList(), Neo4jTypes.NTBoolean, "", new String[]{}, null, false);
        try(Transaction tx = gds.beginTx()) {
            GraphPropertiesProxy props = CypherProcedures.CustomProcedureStorage.getProperties(gds);
            props.setProperty(CypherProcedures.CustomProcedureStorage.APOC_CUSTOM, JsonUtil.writeValueAsString(MapUtil.map(
                "procedures", MapUtil.map(),
                    "functions", MapUtil.map(
                            "myfunc", MapUtil.map(
                                    "statement", "..",
                                    "forceSingle", false,
                                    "signature", signature.toString(),
                                    "description", MapUtil.map("present", false))
                    )
            )));
            tx.success();
        }

        // When I load the stored state..
        CypherProcedures.CustomProcedureStorage store = new CypherProcedures.CustomProcedureStorage(fakeScheduler, gds, fakeLog);
        store.available();

        // Then its ok
        List<CypherProcedures.CustomProcedureInfo> result = store.list();
        assertEquals(1, result.size());
        CypherProcedures.CustomProcedureInfo myFuncInfo = result.get(0);
        assertNull(myFuncInfo.description);
    }
}
