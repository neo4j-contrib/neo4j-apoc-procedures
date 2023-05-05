/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.cluster;

import apoc.Description;
import apoc.Extended;
import apoc.result.GraphResult;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;

@Extended
public class Cluster
{
    @Context
    public Transaction tx;
    @Context
    public GraphDatabaseAPI api;

    private static final String boltAddressKey = "bolt_address";
    private static final Map<String,String> shortName = new HashMap<String,String>()
    {{
        put( "LEADER", "L" );
        put( "FOLLOWER", "F" );
        put( "READ_REPLICA", "RR" );
    }};

    @Procedure
    @Deprecated
    @Description( "apoc.cluster.graph - visually displays the servers participating in the Causal Cluster, their " +
            "roles, and which server in the cluster you are connected to." )
    public Stream<GraphResult> graph()
    {
        Result execute = tx.execute( "CALL dbms.cluster.overview()" );
        List<Node> servers = new LinkedList<>();
        List<Relationship> relationships = new LinkedList<>();

        while ( execute.hasNext() )
        {
            Map<String,Object> next = execute.next();
            String role = (String) next.get( "role" );
            String id = (String) next.get( "id" );
            Label roleLabel = Label.label( role );
            String[] addresses = ((List<String>) next.get( "addresses" )).toArray( new String[0] );
            Map<String,Object> properties = new HashMap<>();
            properties.put( "name", shortName.get( role ) );
            properties.put( "title", role );
            properties.put( boltAddressKey, addresses[0] );
            properties.put( "http_address", addresses[1] );
            properties.put( "cluster_id", id );
            Node server = new VirtualNode( new Label[]{roleLabel}, properties );
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
                new VirtualNode( new Label[]{Label.label( "CLIENT" )}, singletonMap( "name", "Client" ) );
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
        if ( config.get(BoltConnector.enabled) )
        {
            SocketAddress advertisedAddress = config.get(BoltConnector.advertised_address);
            return Optional.of( "neo4j://" + advertisedAddress );
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
