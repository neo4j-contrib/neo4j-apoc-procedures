package apoc.broker;

import java.util.Map;

public class BrokerMessage
{
    public String connectionName;
    public Map<String,Object> message;
    public Map<String,Object> configuration;

    public BrokerMessage()
    {
    }

    public BrokerMessage( String connectionName, Map<String,Object> message, Map<String,Object> configuration )
    {
        this.connectionName = connectionName;
        this.message = message;
        this.configuration = configuration;
    }
}
