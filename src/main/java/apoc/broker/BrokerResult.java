package apoc.broker;

import java.util.Map;

public class BrokerResult
{
    public String connectionName;
    public String messageId;
    public Map<String,Object> message;

    public BrokerResult()
    {
    }

    public BrokerResult( String connectionName, String messageId, Map<String,Object> message )
    {
        this.connectionName = connectionName;
        this.messageId = messageId;
        this.message = message;
    }

    public String getConnectionName()
    {
        return connectionName;
    }

    public void setConnectionName( String connectionName )
    {
        this.connectionName = connectionName;
    }

    public String getMessageId()
    {
        return messageId;
    }

    public void setMessageId( String messageId )
    {
        this.messageId = messageId;
    }

    public Map<String,Object> getMessage()
    {
        return message;
    }

    public void setMessage( Map<String,Object> message )
    {
        this.message = message;
    }
}
