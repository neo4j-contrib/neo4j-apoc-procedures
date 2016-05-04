package apoc.spatial;

import apoc.Description;
import apoc.load.LoadJson;
import apoc.result.MapResult;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import apoc.util.JsonUtil;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static apoc.util.MapUtil.map;

public class Geocode {
    @Context
    public GraphDatabaseService db;

    @Procedure
    @Description("apoc.spatial.geocode('address') YIELD object - look up geographic location of address from geocoding service")
    public Stream<MapResult> geocode(@Name("address") String address) {
        String url = "http://nominatim.openstreetmap.org/search.php?q=" + address.replace(" ", "+") + "&format=json";
        Object value = JsonUtil.loadJson(url);
        if (value instanceof List) {
            return ((List<Map>) value).stream().map(data ->
                    map("latitude", data.get("lat"), "longitude", data.get("lon"),
                        "description", data.get("display_name"), "osm", data)
            ).map(MapResult::new);
        }
        throw new RuntimeException("Can't parse geocoding results "+value);
    }
}
