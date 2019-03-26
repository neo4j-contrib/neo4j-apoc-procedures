package apoc.trigger;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertTrue;

/**
 * @author alexiudice
 * @since 14.07.18
 *
 * Tests for fix of #845.
 *
 * Testing disabled triggers needs to be a different test file from 'TriggerTest.java' since
 *  Trigger classes and methods are static and 'TriggerTest.java' instantiates a class that enables triggers.
 *
 */
public class TriggerDisabledTest
{
    private GraphDatabaseService db;
    private long start;

    @Before
    public void setUp() throws Exception
    {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig( "apoc.trigger.enabled", "false" )
                .newGraphDatabase();
        start = System.currentTimeMillis();
        TestUtil.registerProcedure( db, Trigger.class );
    }

    @After
    public void tearDown()
    {
        if ( db != null )
        {
            db.shutdown();
        }
    }

    @Ignore
    @Test
    public void testTriggerDisabledList() throws Exception
    {
        try
        {
            db.execute( "CALL apoc.trigger.list() YIELD name RETURN name" ).close();
            // If no error is thrown, then the test fails.
            assertTrue( false );
        }
        // Catches NullPointerExceptions since they are a subclass of a RuntimeException
        //  and they were the original error thrown (see #845). We do not expected one, so
        //  it causes test failure.
        catch ( NullPointerException e )
        {
            assertTrue( false );
        }
        // We expect a RuntimeException to be thrown.
        catch ( RuntimeException e )
        {
            // Give the user a specific message that hints at what setting they need to change to fix
            // this problem.
            String msg = e.getMessage();
            assertTrue(msg.indexOf("apoc.trigger.enabled") >= 0);
        }
        // Any other exception causes the test to fail.
        catch ( Exception e )
        {
            assertTrue( false );
        }
    }

    @Test
    public void testTriggerDisabledAdd() throws Exception
    {
        try
        {
            db.execute( "CALL apoc.trigger.add('test-trigger', 'RETURN 1', {phase: 'before'}) YIELD name RETURN name" ).close();
            // If no error is thrown, then the test fails.
            assertTrue( false );
        }
        // Catches NullPointerExceptions since they are a subclass of a RuntimeException
        //  and they were the original error thrown (see #845). We do not expected one, so
        //  it causes test failure.
        catch ( NullPointerException e )
        {
            assertTrue( false );
        }
        // We expect a RuntimeException to be thrown.
        catch ( RuntimeException e )
        {

        }
        // Any other exception causes the test to fail.
        catch ( Exception e )
        {
            assertTrue( false );
        }
    }

    @Test
    public void testTriggerDisabledRemove() throws Exception
    {

        try
        {
            db.execute( "CALL apoc.trigger.REMOVE('test-trigger')" ).close();
            // If no error is thrown, then the test fails.
            assertTrue( false );
        }
        // Catches NullPointerExceptions since they are a subclass of a RuntimeException
        //  and they were the original error thrown (see #845). We do not expected one, so
        //  it causes test failure.
        catch ( NullPointerException e )
        {
            assertTrue( false );
        }
        // We expect a RuntimeException to be thrown.
        catch ( RuntimeException e )
        {

        }
        // Any other exception causes the test to fail.
        catch ( Exception e )
        {
            assertTrue( false );
        }
    }

    @Test
    public void testTriggerDisabledResume() throws Exception
    {

        try
        {
            db.execute( "CALL apoc.trigger.resume('test-trigger')" ).close();
            // If no error is thrown, then the test fails.
            assertTrue( false );
        }
        // Catches NullPointerExceptions since they are a subclass of a RuntimeException
        //  and they were the original error thrown (see #845). We do not expected one, so
        //  it causes test failure.
        catch ( NullPointerException e )
        {
            assertTrue( false );
        }
        // We expect a RuntimeException to be thrown.
        catch ( RuntimeException e )
        {

        }
        // Any other exception causes the test to fail.
        catch ( Exception e )
        {
            assertTrue( false );
        }
    }

    @Test
    public void testTriggerDisabledPause() throws Exception
    {

        try
        {
            db.execute( "CALL apoc.trigger.pause('test-trigger')" ).close();
            // If no error is thrown, then the test fails.
            assertTrue( false );
        }
        // Catches NullPointerExceptions since they are a subclass of a RuntimeException
        //  and they were the original error thrown (see #845). We do not expected one, so
        //  it causes test failure.
        catch ( NullPointerException e )
        {
            assertTrue( false );
        }
        // We expect a RuntimeException to be thrown.
        catch ( RuntimeException e )
        {

        }
        // Any other exception causes the test to fail.
        catch ( Exception e )
        {
            assertTrue( false );
        }
    }
}
