package apoc.agg;

import static apoc.agg.RollupTestUtil.*;
import static apoc.util.ExtendedTestUtil.assertMapEquals;
import static apoc.util.TestUtil.testCall;
import static apoc.util.Util.map;

import apoc.map.Maps;
import apoc.util.TestUtil;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

public class RollupTest {
    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, Maps.class, Rollup.class);

        /*

        Similar to CREATE TABLE Products(SupplierID NUMBER(5,2), CategoryID NUMBER(5,2), Price NUMBER(5,2), otherNum FLOAT(5), anotherID NUMBER(5,2));

        INSERT INTO Products VALUES(1, 1, 1, 18, 0.3);
        INSERT INTO Products VALUES(1, 1, 0, 19, 0.5);
        ...
         */
        String query = "CREATE (:Product {SupplierID: 1, CategoryID: 1, anotherID: 1, Price: 18, otherNum: 0.3}),\n"
                + "   (:Product {SupplierID: 1, CategoryID: 1, anotherID: 0, Price: 19, otherNum: 0.5}),\n"
                + "   (:Product {SupplierID: 1, CategoryID: 2, anotherID: 1, Price: 10, otherNum: 0.6}),\n"
                + "   (:Product {SupplierID: 4, CategoryID: 8, anotherID: 0, Price: 31, otherNum: 0.6}),\n"
                + "   (:Product {SupplierID: 5, CategoryID: 4, anotherID: 1, Price: 21, otherNum: 0.2}),\n"
                + "   (:Product {SupplierID: 6, CategoryID: 8, anotherID: 1, Price: 6, otherNum: 0.5}),\n"
                + "   (:Product {SupplierID: 6, CategoryID: 7, anotherID: 1, Price: 23, otherNum: 0.6}),\n"
                + "   (:Product {SupplierID: 7, CategoryID: 3, anotherID: 1, Price: 17, otherNum: 0.7}),\n"
                + "   (:Product {SupplierID: 7, CategoryID: 6, anotherID: 1, Price: 39, otherNum: 0.8}),\n"
                + "   (:Product {SupplierID: 7, CategoryID: 8, anotherID: 1, Price: 63, otherNum: 0.9}),\n"
                + "   (:Product {SupplierID: 8, CategoryID: 3, anotherID: 0, Price: 9, otherNum: 0.2}),\n"
                + "   (:Product {SupplierID: 8, CategoryID: 3, anotherID: 1, Price: 81, otherNum: 0.5}),\n"
                + "   (:Product {SupplierID: 9, CategoryID: 5, anotherID: 0, Price: 9, otherNum: 0.9}),\n"
                + "   (:Product {SupplierID: 10, CategoryID: 1, anotherID: 1, Price: 5, otherNum: 0.2}),\n"
                + "   (:Product {SupplierID: 11, CategoryID: 3, anotherID: 1, Price: 14.0, otherNum: 0.1}),\n"
                + "   (:Product {SupplierID: 11, CategoryID: 3, anotherID: 0, Price: 31.0, otherNum: 2}),\n"
                + "   (:Product {SupplierID: 11, CategoryID: 4, anotherID: 0, Price: 44, otherNum: 0.7}),\n"
                + "   (:Product {SupplierID: 1, CategoryID: NULL, anotherID: 1, Price: 18, otherNum: 0.7}),\n"
                + "   (:Product {SupplierID: NULL, CategoryID: NULL, anotherID: 0, Price: 18, otherNum: 0.6}),\n"
                + "   (:Product {SupplierID: NULL, CategoryID: 2, anotherID: 0, Price: 199, otherNum: 0.8})";
        db.executeTransactionally(query);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    // similar to https://docs.oracle.com/cd/F49540_01/DOC/server.815/a68003/rollup_c.htm#32084
    // and MySql `WITH ROLLUP` command
    @Test
    public void testRollupRespectivelySupplierIDCategoryIDAndAnotherID() {

        List<Map> expected = getRollupTripleGroup();
        String call = "MATCH (p:Product) RETURN apoc.agg.rollup(p, $groupKeys, [\"Price\", \"otherNum\"]) as data";
        testCall(db, call, map("groupKeys", List.of(SUPPLIER_ID, CATEGORY_ID, ANOTHER_ID)), r -> {
            assertRollupCommon(expected, r);
        });
    }

    @Test
    public void testRollupRespectivelyCategoryIDSupplierIDAndAnotherID() {

        List<Map> expected = getRollupTripleGroupTwo();
        String call = "MATCH (p:Product) RETURN apoc.agg.rollup(p, $groupKeys, [\"Price\", \"otherNum\"]) as data";
        testCall(db, call, map("groupKeys", List.of(CATEGORY_ID, SUPPLIER_ID, ANOTHER_ID)), r -> {
            assertRollupCommon(expected, r);
        });
    }

    @Test
    public void testRollupWithSingleAggregationKey() {

        List<Map> expected = getRollupTripleGroupTwo().stream()
                .peek(i -> {
                    i.remove(sumOtherNum);
                    i.remove(countOtherNum);
                    i.remove(avgOtherNum);
                })
                .collect(Collectors.toList());
        String call = "MATCH (p:Product) RETURN apoc.agg.rollup(p, $groupKeys, [\"Price\"]) as data";
        testCall(db, call, map("groupKeys", List.of(CATEGORY_ID, SUPPLIER_ID, ANOTHER_ID)), r -> {
            assertRollupCommon(expected, r);
        });
    }

    // similar to https://docs.oracle.com/cd/F49540_01/DOC/server.815/a68003/rollup_c.htm#32311
    @Test
    public void testCubeRespectivelySupplierIDCategoryIDAndAnotherID() {

        List<Map> expected = getCubeTripleGroupTwo();
        String call =
                "MATCH (p:Product) RETURN apoc.agg.rollup(p, $groupKeys, ['Price', 'otherNum'], {cube: true}) as data";
        testCall(db, call, map("groupKeys", List.of(CATEGORY_ID, SUPPLIER_ID, ANOTHER_ID)), r -> {
            assertRollupCommon(expected, r);
        });
    }

    @Test
    public void testCube2() {

        List<Map> expected = getCubeTripleGroupTwo().stream()
                .peek(i -> {
                    i.remove(sumOtherNum);
                    i.remove(countOtherNum);
                    i.remove(avgOtherNum);
                })
                .collect(Collectors.toList());
        String call = "MATCH (p:Product) RETURN apoc.agg.rollup(p, $groupKeys, ['Price'], {cube: true}) as data";
        testCall(db, call, map("groupKeys", List.of(CATEGORY_ID, SUPPLIER_ID, ANOTHER_ID)), r -> {
            assertRollupCommon(expected, r);
        });
    }

    private void assertRollupCommon(List<Map> expected, Map<String, Object> r) {
        List<Map> data = (List<Map>) r.get("data");

        for (int i = 0; i < expected.size(); i++) {
            String errMsg = String.format(
                    "Maps at index %s are not equal. \n Expected: %s, \n Actual: %s\n",
                    i, expected.get(i), data.get(i));
            assertMapEquals(errMsg, expected.get(i), data.get(i));
        }
    }
}
