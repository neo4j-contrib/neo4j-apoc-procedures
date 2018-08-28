package apoc.broker;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Name;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class SqsConnectionFactory
{

    private SqsConnectionFactory()
    {
    }

    public static SqsConnection createConnection( String connectionName, Log log, Map<String,Object> configuration )
    {
        return new SqsConnection( log, connectionName, configuration );
    }

    public static class SqsConnection implements BrokerConnection
    {

        private Log log;
        private String connectionName;
        private Map<String,Object> configuration;
        private AmazonSQS amazonSQS;

        public SqsConnection( Log log, String connectionName, Map<String,Object> configuration )
        {
            this.log = log;
            this.connectionName = connectionName;
            this.configuration = configuration;

            amazonSQS = AmazonSQSClientBuilder.standard().withCredentials( new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials( (String) configuration.get( "access.key.id" ), (String) configuration.get( "secret.key.id" ) ) ) ).build();
        }

        @Override
        public Stream<BrokerMessage> send( @Name( "message" ) Map<String,Object> message, @Name( "configuration" ) Map<String,Object> configuration )
        {
            if ( !configuration.containsKey( "queueName" ) )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: 'queueName' in parameters missing" );
            }
            if ( !configuration.containsKey( "region" ) )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: 'region' in parameters missing" );
            }

            String queueName = (String) configuration.get( "queueName" );
            String region = (String) configuration.get( "region" );

            if ( doesQueueExistInRegion( queueName, region ) )
            {
                try
                {
                    amazonSQS.sendMessage( new SendMessageRequest().withQueueUrl( queueName ).withMessageBody( objectMapper.writeValueAsString( message ) ) );
                }
                catch ( Exception e )
                {
                    log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: " + e.toString() );
                }
            }
            else
            {
                log.error(
                        "Broker Exception. Connection Name: " + connectionName + ". Error: SQS queue '" + queueName + "' does not exist in region '" + region +
                                "'." );
            }

            return Stream.of( new BrokerMessage( connectionName, message, configuration ) );
        }

        @Override
        public Stream<BrokerResult> receive( @Name( "configuration" ) Map<String,Object> configuration ) throws IOException
        {
            List<BrokerResult> responseList = new ArrayList<>();

            if ( !configuration.containsKey( "queueName" ) )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: 'queueName' in parameters missing" );
            }
            if ( !configuration.containsKey( "region" ) )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: 'region' in parameters missing" );
            }

            String queueName = (String) configuration.get( "queueName" );
            String region = (String) configuration.get( "region" );

            Long pollRecordsMax = Long.parseLong( maxPollRecordsDefault );
            if ( this.configuration.containsKey( "poll.records.max" ) )
            {
                pollRecordsMax = Long.parseLong( (String) this.configuration.get( "poll.records.max" ) );
            }
            if ( configuration.containsKey( "pollRecordsMax" ) )
            {
                pollRecordsMax = Long.parseLong( (String) configuration.get( "pollRecordsMax" ) );
            }

            if ( pollRecordsMax > 10 || pollRecordsMax < 1 )
            {
                log.error( "Broker Exception. pollRecordsMax for '" + connectionName + "' is either less than 1 or more than 10. Defaulting the value to " +
                        maxPollRecordsDefault + "." );
                pollRecordsMax = Long.parseLong( maxPollRecordsDefault );
            }

            if ( doesQueueExistInRegion( queueName, region ) )
            {
                try
                {
                    ReceiveMessageResult receiveMessageResult = amazonSQS.receiveMessage(
                            new ReceiveMessageRequest().withQueueUrl( queueName ).withMaxNumberOfMessages( pollRecordsMax.intValue() ) );
                    if ( !receiveMessageResult.getMessages().isEmpty() )
                    {
                        for ( Message message : receiveMessageResult.getMessages() )
                        {
                            // Get message and read it as a map.
                            responseList.add(
                                    new BrokerResult( connectionName, message.getMessageId(), objectMapper.readValue( message.getBody(), Map.class ) ) );

                            // Ack and delete message after receiving it.
                            final String messageReceiptHandle = message.getReceiptHandle();
                            amazonSQS.deleteMessage( new DeleteMessageRequest( queueName, messageReceiptHandle ) );
                        }
                    }
                    else
                    {
                        log.error( "Broker Exception. Connection Name: " + connectionName + ". No messages received from SQS queue '" + queueName +
                                "' in region '" + region + "'." );
                    }
                }
                catch ( Exception e )
                {
                    log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: " + e.toString() );
                }
            }
            else
            {
                log.error(
                        "Broker Exception. Connection Name: " + connectionName + ". Error: SQS queue '" + queueName + "' does not exist in region '" + region +
                                "'." );
            }

            return Arrays.stream( responseList.toArray( new BrokerResult[responseList.size()] ) );
        }

        @Override
        public void stop()
        {
            try
            {
                amazonSQS.shutdown();
            }
            catch ( Exception e )
            {
                log.error( "Broker Exception. Failed to stop(). Connection Name: " + connectionName + ". Error: " + e.toString() );
            }
        }

        private Boolean doesQueueExistInRegion( final String queueName, final String region )
        {
            for ( String queueUrl : amazonSQS.listQueues().getQueueUrls() )
            {
                if ( queueUrl.matches( ".*sqs[.]" + StringUtils.lowerCase( region ) + "[.]amazonaws[.]com.*" ) && queueUrl.endsWith( "/" + queueName ) )
                {
                    return true;
                }
            }
            return false;
        }
    }
}
