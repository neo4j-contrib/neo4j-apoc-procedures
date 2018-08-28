package apoc.broker;

import org.neo4j.logging.Log;

import java.util.HashMap;
import java.util.Map;

public class ConnectionManager
{
    private ConnectionManager()
    {
    }

    private static Map<String,BrokerConnection> brokerConnections = new HashMap<>();

    public static BrokerConnection addRabbitMQConnection( String connectionName, Log log, Map<String,Object> configuration )
    {
        log.info( "APOC Broker: Adding RabbitMQ Connection '" + connectionName + "' with configurations " + configuration.toString() );
        return brokerConnections.put( connectionName, RabbitMqConnectionFactory.createConnection( connectionName, log, configuration ) );
    }

    public static BrokerConnection addSQSConnection( String connectionName, Log log, Map<String,Object> configuration )
    {
        log.info( "APOC Broker: Adding SQS Connection '" + connectionName + "' with configurations " + configuration.toString() );
        return brokerConnections.put( connectionName, SqsConnectionFactory.createConnection( connectionName, log, configuration ) );
    }

    public static BrokerConnection addKafkaConnection( String connectionName, Log log, Map<String,Object> configuration )
    {
        log.info( "APOC Broker: Adding Kafka Connection '" + connectionName + "' with configurations " + configuration.toString() );
        return brokerConnections.put( connectionName, KafkaConnectionFactory.createConnection( connectionName, log, configuration ) );
    }

    public static void closeConnections()
    {
        brokerConnections.forEach( (name,connection) -> connection.stop());
    }

    public static Map<String,BrokerConnection> getBrokerConnections()
    {
        return brokerConnections;
    }
}
