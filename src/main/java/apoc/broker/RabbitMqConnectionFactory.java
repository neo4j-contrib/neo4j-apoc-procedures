package apoc.broker;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Name;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

public class RabbitMqConnectionFactory
{

    private RabbitMqConnectionFactory()
    {
    }

    public static RabbitMqConnection createConnection( String connectionName, Log log, Map<String,Object> configuration )
    {
        return new RabbitMqConnection( log, connectionName, configuration );
    }

    public static class RabbitMqConnection implements BrokerConnection
    {
        private Log log;
        private String connectionName;
        private Map<String,Object> configuration;
        private ConnectionFactory connectionFactory = new ConnectionFactory();
        private Connection connection;
        private Channel channel;

        public RabbitMqConnection( Log log, String connectionName, Map<String,Object> configuration )
        {
            this.log = log;
            this.connectionName = connectionName;
            this.configuration = configuration;
            try
            {
                this.connectionFactory.setUsername( (String) configuration.get( "username" ) );
                this.connectionFactory.setPassword( (String) configuration.get( "password" ) );
                this.connectionFactory.setVirtualHost( (String) configuration.get( "vhost" ) );
                this.connectionFactory.setHost( (String) configuration.get( "host" ) );
                this.connectionFactory.setPort( Integer.parseInt( (String) configuration.get( "port" ) ) );

                this.connection = this.connectionFactory.newConnection();

                this.channel = this.connection.createChannel();
            }
            catch ( Exception e )
            {
                this.log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: " + e.toString() );
            }
        }

        @Override
        public Stream<BrokerMessage> send( @Name( "message" ) Map<String,Object> message, @Name( "configuration" ) Map<String,Object> configuration )
        {
            if ( !configuration.containsKey( "exchangeName" ) )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: 'exchangeName' in parameters missing" );
            }
            if ( !configuration.containsKey( "queueName" ) )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: 'queueName' in parameters missing" );
            }
            if ( !configuration.containsKey( "routingKey" ) )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: 'routingKey' in parameters missing" );
            }

            String exchangeName = (String) configuration.get( "exchangeName" );
            String queueName = (String) configuration.get( "queueName" );
            String routingKey = (String) configuration.get( "routingKey" );
            try
            {
                // Ensure the exchange and queue are declared.
                channel.exchangeDeclare( exchangeName, "topic", true );
                channel.queueDeclarePassive( queueName );

                // Ensure the exchange and queue are bound by the routing key.
                channel.queueBind( queueName, exchangeName, routingKey );

                // Get the message bytes and send the message bytes.
                channel.basicPublish( exchangeName, routingKey, null, objectMapper.writeValueAsBytes( message ) );
            }
            catch ( Exception e )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: " + e.toString() );
            }

            return Stream.of( new BrokerMessage( connectionName, message, configuration ) );
        }

        @Override
        public Stream<BrokerResult> receive( @Name( "configuration" ) Map<String,Object> configuration ) throws IOException
        {
            if ( !configuration.containsKey( "queueName" ) )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: 'queueName' in parameters missing" );
            }

            Long pollRecordsMax = Long.parseLong( maxPollRecordsDefault );
            if ( this.configuration.containsKey( "poll.records.max" ) )
            {
                pollRecordsMax = Long.parseLong( (String) this.configuration.get( "poll.records.max" ) );
            }
            if ( configuration.containsKey( "pollRecordsMax" ) )
            {
                pollRecordsMax = Long.parseLong( (String) configuration.get( "pollRecordsMax" ) );
            }

            List<GetResponse> messageList = new ArrayList<>();
            List<BrokerResult> messageMap = new ArrayList<>();

            synchronized ( channel )
            {
                try
                {
                    if ( pollRecordsMax == 0L )
                    {
                        pollRecordsMax = channel.messageCount( (String) configuration.get( "queueName" ) );
                    }

                    for ( int i = 0; i < pollRecordsMax; i++ )
                    {
                        GetResponse message = channel.basicGet( (String) configuration.get( "queueName" ), false );
                        if ( message == null )
                        {
                            log.error( "Broker Exception. Connection Name: " + connectionName + ". Message retrieved is null. Possibly no messages in the '" +
                                    configuration.get( "queueName" ) + "' queue." );
                            break;
                        }
                        messageList.add( message );
                    }

                    for ( GetResponse getResponse : messageList )
                    {

                        messageMap.add( new BrokerResult( connectionName, Long.toString( getResponse.getEnvelope().getDeliveryTag() ),
                                objectMapper.readValue( getResponse.getBody(), Map.class ) ) );

                        // Ack the message
                        channel.basicAck( getResponse.getEnvelope().getDeliveryTag(), false );
                    }
                }
                catch ( Exception e )
                {
                    log.error( "Broker Exception. Connection Name: " + connectionName + ". Exception when trying to get a message from the '" +
                            configuration.get( "queueName" ) + "' queue." );
                    throw e;
                }
            }

            return Arrays.stream( messageMap.toArray( new BrokerResult[messageMap.size()] ) );
        }

        @Override
        public void stop()
        {
            try
            {
                channel.close();
                connection.close();
            }
            catch ( Exception e )
            {
                log.error( "Broker Exception. Failed to stop(). Connection Name: " + connectionName + ". Error: " + e.toString() );
            }
        }

        private void createChannelIfClosed() throws IOException
        {
            if ( !channel.isOpen() )
            {
                channel = connection.createChannel();
            }
        }

        private void createConnectionIfClosed() throws IOException, TimeoutException
        {
            if ( !connection.isOpen() )
            {
                this.connection = connectionFactory.newConnection();
            }
        }
    }
}
