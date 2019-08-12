package apoc.custom;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.EmbeddedDbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 18.08.18
 */
public class CypherProceduresStorageTest {

    public static final File STORE_DIR = new File("cypher-storage");

    @Rule
    public DbmsRule db = new EmbeddedDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, CypherProcedures.class);
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deleteRecursively(STORE_DIR);
    }

    private void restartDb() throws IOException {
        db.restartDatabase();
        TestUtil.registerProcedure(db, CypherProcedures.class); // TODO: maybe that's needed
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
}
