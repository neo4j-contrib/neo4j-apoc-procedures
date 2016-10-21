package apoc.cluster;

import apoc.Description;
import apoc.result.GraphResult;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;

import static java.util.Collections.singletonMap;

public class Cluster
{
    @Context
    public GraphDatabaseService db;
    @Context
    public GraphDatabaseAPI api;

    public static final String boltAddressKey = "bolt_address";
    public static final Map<String,String> shortName = new HashMap<String,String>()
    {{
        put( "LEADER", "L" );
        put( "FOLLOWER", "F" );
        put( "READ_REPLICA", "RR" );
    }};

    @Procedure
    @Description( "apoc.cluster.graph - visually displays the servers participating in the Causal Cluster, their " +
            "roles, and which server in the cluster you are connected to." )
    public Stream<GraphResult> graph()
    {
        Result execute = db.execute( "CALL dbms.cluster.overview()" );
        List<Node> servers = new LinkedList<>();
        List<Relationship> relationships = new LinkedList<>();

        while ( execute.hasNext() )
        {
            Map<String,Object> next = execute.next();
            String role = (String) next.get( "role" );
            String id = (String) next.get( "id" );
            Label roleLabel = Label.label( role );
            String[] addresses = Arrays.stream( (Object[]) next.get( "addresses" ) ).toArray( String[]::new );
            Map<String,Object> properties = new HashMap<>();
            properties.put( "name", shortName.get( role ) );
            properties.put( "title", role );
            properties.put( boltAddressKey, addresses[0] );
            properties.put( "http_address", addresses[1] );
            properties.put( "cluster_id", id );
            Node server = new VirtualNode( new Label[]{roleLabel}, properties, db );
            servers.add( server );
        }

        Optional<Node> leaderNode = getLeaderNode( servers );
        if ( leaderNode.isPresent() )
        {
            for ( Node server : servers )
            {
                if ( server.hasLabel( Label.label( "FOLLOWER" ) ) )
                {
                    VirtualRelationship follows =
                            new VirtualRelationship( server, leaderNode.get(), RelationshipType.withName( "FOLLOWS" ) );
                    relationships.add( follows );
                }
            }
        }

        VirtualNode client =
                new VirtualNode( new Label[]{Label.label( "CLIENT" )}, singletonMap( "name", "Client" ), db );
        Optional<Relationship> clientConnection = determineClientConnection( servers, client );
        if ( clientConnection.isPresent() )
        {
            servers.add( client );
            relationships.add( clientConnection.get() );
        }

        GraphResult graphResult = new GraphResult( servers, relationships );
        return Stream.of( graphResult );
    }

    private Optional<Relationship> determineClientConnection( List<Node> servers, Node client )
    {
        Optional<String> boltAddress = getBoltConnector();
        if ( boltAddress.isPresent() )
        {
            for ( Node server : servers )
            {
                if ( serverHasBoltAddress( boltAddress.get(), server ) )
                {
                    return Optional.of( new VirtualRelationship( client, server,
                            RelationshipType.withName( "CONNECTED_TO" ) ) );
                }
            }
        }
        return Optional.empty();
    }

    private Optional<String> getBoltConnector()
    {
        Config config = api.getDependencyResolver().resolveDependency( Config.class );
        List<GraphDatabaseSettings.BoltConnector> boltConnectors = GraphDatabaseSettings.boltConnectors( config );
        Optional<GraphDatabaseSettings.BoltConnector> boltConnector = boltConnectors.stream().findFirst();
        if ( boltConnector.isPresent() )
        {
            AdvertisedSocketAddress from = boltConnector.get().advertised_address.from( config );
            return Optional.of( "bolt://" + from );
        }
        return Optional.empty();
    }

    private boolean serverHasBoltAddress( String boltAddress, Node server )
    {
        String address = (String) server.getProperty( boltAddressKey );
        return address.equals( boltAddress );
    }


    private Optional<Node> getLeaderNode( List<Node> servers )
    {
        for ( Node server : servers )
        {
            if ( server.hasLabel( Label.label( "LEADER" ) ) )
            {
                return Optional.of( server );
            }
        }
        return Optional.empty();
    }
}
