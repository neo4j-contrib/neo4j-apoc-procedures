package apoc.cluster;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.procedures.ClusterOverviewProcedure;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.test.causalclustering.ClusterRule;

import static apoc.cluster.Cluster.boltAddressKey;
import static org.junit.Assert.assertEquals;

public class ClusterTest
{
    private org.neo4j.causalclustering.discovery.Cluster cluster;

    @Rule
    public ClusterRule clusterRule = new ClusterRule( getClass() );

    @Before
    public void setUp() throws Exception
    {

        cluster = clusterRule.startCluster();
        for ( CoreClusterMember member : cluster.coreMembers() )
        {
            TestUtil.registerProcedure( member.database(), Cluster.class, ClusterOverviewProcedure.class );
        }
    }

    @Test
    public void testClusterOverview() throws Exception
    {
        CoreClusterMember coreMember = cluster.getCoreMemberById( 0 );
        TestUtil.testResult( coreMember.database(), "CALL apoc.cluster.graph()", ( result ) ->
        {
            while ( result.hasNext() )
            {
                Map<String,Object> next = result.next();
                List nodes = (List) next.get( "nodes" );
                Iterator iterator = nodes.iterator();
                Node node;
                while ( iterator.hasNext() )
                {
                    node = (Node) iterator.next();
                    if ( node.hasLabel( Label.label( "CLIENT" ) ) )
                    {
                        iterator.remove();
                        continue;
                    }

                    HostnamePort boltAddress = new HostnamePort( (String) node.getProperty( boltAddressKey ) );
                    AdvertisedSocketAddress address =
                            new AdvertisedSocketAddress( boltAddress.getHost(), boltAddress.getPort() );
                    try
                    {
                        cluster.getMemberByBoltAddress( address );
                        //Since the line above didn't send us to the catch block, we have a match.
                        iterator.remove();
                    }
                    catch ( RuntimeException e )
                    {
                        //expected for ones we don't match.
                    }
                }
                assertEquals( 0, nodes.size() ); //we matched all nodes from the procedure to the test cluster.

                List<Relationship> relationships = (List) next.get( "relationships" );
                for ( Relationship relationship : relationships )
                {
                    if ( relationship.getType().equals( RelationshipType.withName( "CONNECTED_TO" ) ) )
                    {
                        assertEquals( "bolt://" + coreMember.boltAdvertisedAddress(),
                                ( relationship.getEndNode().getProperty( "bolt_address" )) );
                    }
                }
            }
        } );
    }
}
