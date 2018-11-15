package apoc.load.util;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Map;

/**
 * @author ab-Larus
 * @since 03-10-18
 */
public class LoadJdbcConfig {

    private ZoneId zoneId = null;

    public LoadJdbcConfig(Map<String,Object> config) {
        config = config != null ? config : Collections.emptyMap();
        try {
            this.zoneId = config.containsKey("timezone") ?
                    ZoneId.of(config.get("timezone").toString()) : null;
        } catch (DateTimeException e) {
            throw new IllegalArgumentException(String.format("The timezone field contains an error: %s", e.getMessage()));
        }
    }

    public ZoneId getZoneId(){
        return this.zoneId;
    }

}