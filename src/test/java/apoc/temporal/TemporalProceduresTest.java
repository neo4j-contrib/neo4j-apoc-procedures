package apoc.temporal;

import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;

import static org.junit.Assert.assertEquals;

public class TemporalProceduresTest
{

    @Rule
    public final Neo4jRule neo4j = new Neo4jRule()
            .withFunction(TemporalProcedures.class);

    @Test
    public void shouldFormatDate() throws Throwable
    {
        GraphDatabaseService db = neo4j.getGraphDatabaseService();

        try (Transaction tx = db.beginTx() ) {
            Result res = db.execute("RETURN apoc.temporal.format( date( { year: 2018, month: 12, day: 10 } ), \"yyyy-MM-dd\" ) as output");

            assertEquals( res.next().get("output"), "2018-12-10" );
        }
    }


    @Test
    public void shouldFormatDateTime() throws Throwable
    {
        GraphDatabaseService db = neo4j.getGraphDatabaseService();

        try (Transaction tx = db.beginTx() ) {
            Result res = db.execute("RETURN apoc.temporal.format( datetime( { year: 2018, month: 12, day: 10, hour: 12, minute: 34, second: 56, nanosecond: 123456789 } ), \"yyyy-MM-dd'T'HH:mm:ss.SSSS\" ) as output");

            assertEquals( res.next().get("output"), "2018-12-10T12:34:56.1234" );
        }
    }

    @Test
    public void shouldFormatLocalDateTime() throws Throwable
    {
        GraphDatabaseService db = neo4j.getGraphDatabaseService();

        try (Transaction tx = db.beginTx() ) {
            Result res = db.execute("RETURN apoc.temporal.format( localdatetime( { year: 2018, month: 12, day: 10, hour: 12, minute: 34, second: 56, nanosecond: 123456789 } ), \"yyyy-MM-dd'T'HH:mm:ss.SSSS\" ) as output");

            assertEquals( res.next().get("output"), "2018-12-10T12:34:56.1234" );
        }
    }

    @Test
    public void shouldFormatTime() throws Throwable
    {
        GraphDatabaseService db = neo4j.getGraphDatabaseService();

        try (Transaction tx = db.beginTx() ) {
            Result res = db.execute("RETURN apoc.temporal.format( time( { hour: 12, minute: 34, second: 56, nanosecond: 123456789, timezone: 'Europe/London' } ), \"HH:mm:ss.SSSSZ\" ) as output");

            assertEquals( res.next().get("output"), "12:34:56.1234+0100" );
        }
    }

    @Test
    public void shouldFormatLocalTime() throws Throwable
    {
        GraphDatabaseService db = neo4j.getGraphDatabaseService();

        try (Transaction tx = db.beginTx() ) {
            Result res = db.execute("RETURN apoc.temporal.format( localtime( { hour: 12, minute: 34, second: 56, nanosecond: 123456789 } ), \"HH:mm:ss.SSSS\" ) as output");

            assertEquals( res.next().get("output"), "12:34:56.1234" );
        }
    }


    @Test
    public void shouldFormatDuration() throws Throwable
    {
        GraphDatabaseService db = neo4j.getGraphDatabaseService();

        try (Transaction tx = db.beginTx() ) {
            Result res = db.execute("RETURN apoc.temporal.format( duration('P0M0DT4820.487660000S'), \"HH:mm:ss.SSSS\" ) as output");

            assertEquals( res.next().get("output"), "01:20:20.4876" );
        }
    }




}
