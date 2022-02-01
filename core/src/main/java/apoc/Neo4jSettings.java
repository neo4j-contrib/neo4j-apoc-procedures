package apoc;

import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddressString;

import java.util.List;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.SettingValueParser;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;

import static apoc.ApocConfig.CYPHER_IP_BLOCKLIST;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.listOf;

/**
 * This should be deprecated with a new major release of Neo4j, namely 5.0, as it was added to
 * avoid binary incompatibilities between APOC and older versions of Neo4j 4.xx.yy
 * This setting was added in Neo4j 4.4.5
 */
@ServiceProvider
public class Neo4jSettings implements SettingsDeclaration {
    public static final SettingValueParser<IPAddressString> CIDR_IP = new SettingValueParser<>()
    {
        @Override
        public IPAddressString parse( String value )
        {
            IPAddressString ipAddress = new IPAddressString( value.trim() );
            try
            {
                ipAddress.validate();
            }
            catch ( AddressStringException e )
            {
                throw new IllegalArgumentException( String.format( "'%s' is not a valid CIDR ip", value ), e );
            }
            return ipAddress;
        }

        @Override
        public String getDescription()
        {
            return "an ip with subnet in CDIR format. e.g. 127.168.0.1/8";
        }

        @Override
        public Class<IPAddressString> getType()
        {
            return IPAddressString.class;
        }
    };

    public static final Setting<List<IPAddressString>> cypher_ip_blocklist = newBuilder( CYPHER_IP_BLOCKLIST, listOf( CIDR_IP ), List.of() ).build();
}

