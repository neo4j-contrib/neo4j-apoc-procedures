package apoc.math;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.harness.junit.Neo4jRule;

import static org.junit.Assert.assertEquals;

/**
 * @author <a href="mailto:ali.arslan@rwth-aachen.de">AliArslan</a>
 */
public class RegressionTest {

    // This rule starts a Neo4j instance for us
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()

            // This is the Procedure we want to test
            .withProcedure( Regression.class );

    @Test
    public void testCalculateRegr() throws Throwable
    {
        // In a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase.driver( neo4j.boltURI() ,
                Config.build().withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() ) )
        {

            Session session = driver.session();

            session.run( "CREATE (p:REGR_TEST {x_property: 1 , y_property: 2 }) RETURN id(p)" );
            session.run( "CREATE (p:REGR_TEST {x_property: 2 , y_property: 3 }) RETURN id(p)" );
            session.run( "CREATE (p:REGR_TEST {y_property: 10000 }) RETURN id(p)" );
            session.run( "CREATE (p:REGR_TEST {x_property: 3 , y_property: 6 }) RETURN id(p)" );

            StatementResult result = session
                    .run( "CALL apoc.math.regr('REGR_TEST', " +
                                        "'y_property', 'x_property')" );

            SimpleRegression expectedRegr = new SimpleRegression(false);
            expectedRegr.addData(new double[][] {
                    {1, 1},
                    {2, 3},
                    //{3, 10000},
                    {3, 6}
            });

            assertEquals(expectedRegr.getRSquare(), result.peek().get("r2").asDouble(), 0.1 );

            assertEquals(2.0, result.peek().get("avgX").asDouble(), 0.1);

            assertEquals(3.67, result.peek().get("avgY").asDouble(), 0.1);

            assertEquals(expectedRegr.getSlope(), result.peek().get("slope").asDouble(), 0.1);
        }
    }

    @Test
    public void testRegrR2isOne() throws Throwable
    {
        // In a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase.driver( neo4j.boltURI() ,
                Config.build().withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() ) )
        {
            Session session = driver.session();

            session.run( "CREATE (p:REGR_TEST {x_property: 1 , y_property: 1 }) RETURN id(p)" );
            session.run( "CREATE (p:REGR_TEST {x_property: 1 , y_property: 1 }) RETURN id(p)" );
            session.run( "CREATE (p:REGR_TEST {y_property: 10000 }) RETURN id(p)" );
            session.run( "CREATE (p:REGR_TEST {x_property: 1 , y_property: 1 }) RETURN id(p)" );


            StatementResult result = session
                    .run( "CALL apoc.math.regr('REGR_TEST', " +
                                        "'y_property', 'x_property')" );

            SimpleRegression expectedRegr = new SimpleRegression(false);
            expectedRegr.addData(new double[][] {
                    {1, 1},
                    {1, 1},
                    //{3, 10000},
                    {1, 1}
            });

            assertEquals(expectedRegr.getRSquare(), result.peek().get("r2").asDouble(), 0.1 );
        }
    }
}
