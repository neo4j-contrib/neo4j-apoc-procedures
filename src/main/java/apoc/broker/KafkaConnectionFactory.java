package apoc.broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Name;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

public class KafkaConnectionFactory
{

    private KafkaConnectionFactory()
    {
    }

    public static KafkaConnection createConnection( String connectionName, Log log, Map<String,Object> configuration )
    {
        return new KafkaConnection( log, connectionName, configuration );
    }

    public static class KafkaConnection implements BrokerConnection
    {
        private static final Integer pollSecondsDefault = 1;
        private Log log;
        private String connectionName;
        private Map<String,Object> configuration;
        private KafkaProducer<String,byte[]> kafkaProducer;
        private KafkaConsumer<String,byte[]> kafkaConsumer;

        public KafkaConnection( Log log, String connectionName, Map<String,Object> configuration )
        {
            this.log = log;
            this.connectionName = connectionName;
            this.configuration = configuration;

            try
            {
                Properties producerProperties = new Properties();
                producerProperties.setProperty( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, (String) configuration.get( "bootstrap.servers" ) );
                producerProperties.setProperty( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer" );
                producerProperties.setProperty( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer" );

                kafkaProducer = new KafkaProducer<String,byte[]>( producerProperties );

                Properties consumerProperties = new Properties();
                consumerProperties.setProperty( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, (String) configuration.get( "bootstrap.servers" ) );
                consumerProperties.setProperty( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer" );
                consumerProperties.setProperty( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer" );
                consumerProperties.setProperty( ConsumerConfig.GROUP_ID_CONFIG, (String) configuration.get( "group.id" ) );

                if ( configuration.containsKey( "client.id" ) )
                {
                    consumerProperties.setProperty( ConsumerConfig.CLIENT_ID_CONFIG, (String) configuration.get( "client.id" ) );
                }

                if ( configuration.containsKey( "poll.records.max" ) )
                {
                    consumerProperties.setProperty( ConsumerConfig.MAX_POLL_RECORDS_CONFIG, (String) configuration.get( "poll.records.max" ) );
                }

                kafkaConsumer = new KafkaConsumer<String,byte[]>( consumerProperties );
            }
            catch ( Exception e )
            {
                this.log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: " + e.toString() );
            }
        }

        @Override
        public Stream<BrokerMessage> send( @Name( "message" ) Map<String,Object> message, @Name( "configuration" ) Map<String,Object> parameters )
        {
            // Topic and value are required
            if ( !parameters.containsKey( "topic" ) )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: 'topic' in parameters missing" );
            }

            String topic = (String) parameters.get( "topic" );

            Integer partition = -1;
            if ( parameters.containsKey( "partition" ) )
            {
                partition = (Integer) parameters.get( "partition" );
            }

            String key = "";
            if ( parameters.containsKey( "key" ) )
            {
                key = (String) parameters.get( "key" );
            }

            try
            {
                ProducerRecord<String,byte[]> producerRecord;
                if ( partition >= 0 && !key.isEmpty() )
                {
                    producerRecord = new ProducerRecord<>( topic, partition, key, objectMapper.writeValueAsBytes( message ) );
                }
                else if ( !key.isEmpty() )
                {
                    producerRecord = new ProducerRecord<>( topic, key, objectMapper.writeValueAsBytes( message ) );
                }
                else
                {
                    producerRecord = new ProducerRecord<>( topic, objectMapper.writeValueAsBytes( message ) );
                }

                kafkaProducer.send( producerRecord );
            }
            catch ( Exception e )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: " + e.toString() );
            }

            return Stream.of( new BrokerMessage( connectionName, message, parameters ) );
        }

        @Override
        public Stream<BrokerResult> receive( @Name( "configuration" ) Map<String,Object> configuration ) throws IOException
        {

            List<BrokerResult> responseList = new ArrayList<>();

            // Topic is required
            if ( !configuration.containsKey( "topic" ) )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: 'topic' in configuration missing" );
            }

            Integer pollSecondsDefault = this.pollSecondsDefault;
            if ( this.configuration.containsKey( "poll.seconds" ) )
            {
                pollSecondsDefault = Integer.parseInt( (String) this.configuration.get( "poll.seconds" ) );
            }
            if ( configuration.containsKey( "pollSeconds" ) )
            {
                pollSecondsDefault = Integer.parseInt( (String) configuration.get( "pollSeconds" ) );
            }

            synchronized ( kafkaConsumer )
            {
                if ( !kafkaConsumer.subscription().contains( (String) configuration.get( "topic" ) ) )
                {
                    kafkaConsumer.subscribe( Collections.singletonList( (String) configuration.get( "topic" ) ) );
                }

                final ConsumerRecords<String,byte[]> consumerRecords = kafkaConsumer.poll( Duration.ofSeconds( pollSecondsDefault ) );

                consumerRecords.forEach( record ->
                {
                    try
                    {
                        responseList.add(
                                new BrokerResult( connectionName, Long.toString( record.offset() ), objectMapper.readValue( record.value(), Map.class ) ) );
                    }
                    catch ( Exception e )
                    {
                        log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: " + e.toString() );
                    }
                } );

                kafkaConsumer.commitAsync();
            }

            return Arrays.stream( responseList.toArray( new BrokerResult[responseList.size()] ) );
        }

        @Override
        public void stop()
        {
            try
            {
                kafkaProducer.close();
                kafkaConsumer.close();
            }
            catch ( Exception e )
            {
                log.error( "Broker Exception. Failed to stop(). Connection Name: " + connectionName + ". Error: " + e.toString() );
            }
        }
    }
}
