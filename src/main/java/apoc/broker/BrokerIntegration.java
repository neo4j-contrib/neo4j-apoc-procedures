package apoc.broker;

import apoc.ApocConfiguration;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class BrokerIntegration
{

    @Procedure( mode = Mode.READ )
    @Description( "apoc.broker.send(connectionName, message, configuration) - Send a message to the broker associated with the connectionName namespace. Takes in parameter which are dependent on the broker being used." )
    public Stream<BrokerMessage> send( @Name( "connectionName" ) String connectionName, @Name( "message" ) Map<String,Object> message,
            @Name( "configuration" ) Map<String,Object> configuration ) throws IOException
    {

        return BrokerHandler.sendMessageToBrokerConnection( connectionName, message, configuration );
    }

    @Procedure( mode = Mode.READ )
    @Description( "apoc.broker.receive(connectionName, configuration) - Receive a message from the broker associated with the connectionName namespace. Takes in a configuration map which is dependent on the broker being used." )
    public Stream<BrokerResult> receive( @Name( "connectionName" ) String connectionName, @Name( "configuration" ) Map<String,Object> configuration )
            throws IOException
    {

        return BrokerHandler.receiveMessageFromBrokerConnection( connectionName, configuration );
    }

    public enum BrokerType
    {
        RABBITMQ,
        SQS,
        KAFKA
    }

    public static class BrokerHandler
    {
        private static Map<String,BrokerConnection> brokerConnections;

        public BrokerHandler( Map<String,BrokerConnection> brokerConnections )
        {
            this.brokerConnections = brokerConnections;
        }

        public static Stream<BrokerMessage> sendMessageToBrokerConnection( String connection, Map<String,Object> message, Map<String,Object> configuration )
                throws IOException
        {
            if ( !brokerConnections.containsKey( connection ) )
            {
                throw new IOException( "Broker Exception. Connection '" + connection + "' is not a configured broker connection." );
            }
            return (brokerConnections.get( connection )).send( message, configuration );
        }

        public static Stream<BrokerResult> receiveMessageFromBrokerConnection( String connection, Map<String,Object> configuration ) throws IOException
        {
            if ( !brokerConnections.containsKey( connection ) )
            {
                throw new IOException( "Broker Exception. Connection '" + connection + "' is not a configured broker connection." );
            }
            return brokerConnections.get( connection ).receive( configuration );
        }
    }

    public static class BrokerLifeCycle
    {
        private final Log log;

        public BrokerLifeCycle( Log log )
        {
            this.log = log;
        }

        private static String getBrokerConfiguration( String connectionName, String key )
        {
            Map<String,Object> value = ApocConfiguration.get( "broker." + connectionName );

            if ( value == null )
            {
                throw new RuntimeException( "No apoc.broker." + connectionName + " specified" );
            }
            return (String) value.get( key );
        }

        public void start()
        {
            Map<String,Object> value = ApocConfiguration.get( "broker" );

            Set<String> connectionList = new HashSet<>();

            value.forEach( ( configurationString, object ) ->
            {
                String connectionName = configurationString.split( "\\." )[0];
                connectionList.add( connectionName );
            } );

            for ( String connectionName : connectionList )
            {

                BrokerType brokerType = BrokerType.valueOf( StringUtils.upperCase( getBrokerConfiguration( connectionName, "type" ) ) );
                Boolean enabled = Boolean.valueOf( getBrokerConfiguration( connectionName, "enabled" ) );

                if ( enabled )
                {
                    switch ( brokerType )
                    {
                    case RABBITMQ:
                        ConnectionManager.addRabbitMQConnection( connectionName, log, ApocConfiguration.get( "broker." + connectionName ) );
                        break;
                    case SQS:
                        ConnectionManager.addSQSConnection( connectionName, log, ApocConfiguration.get( "broker." + connectionName ) );
                        break;
                    case KAFKA:
                        ConnectionManager.addKafkaConnection( connectionName, log, ApocConfiguration.get( "broker." + connectionName ) );
                        break;
                    default:
                        break;
                    }
                }
            }

            new BrokerHandler( ConnectionManager.getBrokerConnections() );
        }

        public void stop()
        {
            ConnectionManager.closeConnections();
        }
    }
}
