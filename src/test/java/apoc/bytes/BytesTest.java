package apoc.bytes;

import apoc.coll.Coll;
import apoc.convert.Json;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.*;

public class BytesTest {

    private static GraphDatabaseService db;

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Bytes.class);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    public static final Bytes BYTES = new Bytes();

    @Test
    public void fromHexString() {
        byte[] expected = new byte[]{(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE};
        byte[] actual = new Bytes().fromHexString("CaFEBaBE");
        assertArrayEquals("expected CaFEBaBE byte array", expected, actual);
    }

    @Test
    public void fromHexStringOddlLength() {
        byte[] expected = new byte[]{(byte) 0xAB, (byte) 0x0C};
        byte[] actual = new Bytes().fromHexString("ABC");
        assertArrayEquals("expected ABC byte array", expected, actual);
    }

    @Test
    public void fromHexStringNull() {
        assertNull("on null argument", BYTES.fromHexString(null));
        assertNull("on empty argument", BYTES.fromHexString(""));
        assertNull("on illegal hex string", BYTES.fromHexString("Hello"));
    }


    @Test
    public void fromHexStringUsingFunction() {
        Map<String, Object> parameters = Collections.singletonMap("text", "CAFEBABE");
        String query = "RETURN  apoc.bytes.fromHexString($text) AS value";

        ResourceIterator<byte[]> rows = db.execute(query, parameters).columnAs("value");

        byte[] expected = new byte[]{(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE};
        assertArrayEquals("expected CAFEBABE byte array", expected, rows.next());
        assertFalse(rows.hasNext());
    }

}