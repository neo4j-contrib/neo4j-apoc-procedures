package apoc.math;

import apoc.util.TestUtil;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static org.junit.Assert.assertEquals;

/**
 * @author <a href="mailto:ali.arslan@rwth-aachen.de">AliArslan</a>
 */
public class RegressionTest {

    @ClassRule
public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, Regression.class);
    }

    @Test
    public void testCalculateRegr() throws Throwable {
        db.executeTransactionally("CREATE " +
                "(:REGR_TEST {x_property: 1 , y_property: 2 })," +
                "(:REGR_TEST {x_property: 2 , y_property: 3 })," +
                "(:REGR_TEST {y_property: 10000 })," +
                "(:REGR_TEST {x_property: 3 , y_property: 6 })");

        SimpleRegression expectedRegr = new SimpleRegression(false);
        expectedRegr.addData(new double[][]{
                {1, 1},
                {2, 3},
                //{3, 10000},
                {3, 6}
        });
        TestUtil.testCall(db, "CALL apoc.math.regr('REGR_TEST', 'y_property', 'x_property')",
                result -> {
                    assertEquals(expectedRegr.getRSquare(), (Double)result.get("r2"), 0.1);
                    assertEquals(2.0, (Double)result.get("avgX"), 0.1);
                    assertEquals(3.67, (Double)result.get("avgY"), 0.1);
                    assertEquals(expectedRegr.getSlope(), (Double)result.get("slope"), 0.1);
        });
    }

    @Test
    public void testRegrR2isOne() throws Throwable {
        db.executeTransactionally("CREATE " +
                "(:REGR_TEST2 {x_property: 1 , y_property: 1 })," +
                "(:REGR_TEST2 {x_property: 1 , y_property: 1 })," +
                "(:REGR_TEST2 {y_property: 10000 })," +
                "(:REGR_TEST2 {x_property: 1 , y_property: 1 })");

        SimpleRegression expectedRegr = new SimpleRegression(false);
        expectedRegr.addData(new double[][]{
                {1, 1},
                {1, 1},
                //{3, 10000},
                {1, 1}
        });

        TestUtil.testCall(db, "CALL apoc.math.regr('REGR_TEST2', 'y_property', 'x_property')",
                result -> {
                    assertEquals(expectedRegr.getRSquare(), (Double)result.get("r2"), 0.1);
                    assertEquals(expectedRegr.getSlope(), (Double)result.get("slope"), 0.1);
                });
    }
}
