package apoc.gephi;

import org.neo4j.graphdb.Node;

import java.util.Iterator;
import java.util.Set;
import java.util.function.BiPredicate;

public class GephiFormatUtils {

    public static String getCaption(Node n, Set<String> captions) {
        for (String caption : captions) { //first do one loop with the exact names
            if (n.hasProperty(caption))
                return n.getProperty(caption).toString();
        }

        String result = filterCaption(n, captions, (key, caption) -> key.equalsIgnoreCase(caption)); //2nd loop with lowercase
        if (result == null) {
            result = filterCaption(n, captions, (key, caption) -> key.toLowerCase().contains(caption) || key.toLowerCase().endsWith(caption)); //3rd loop with contains or endsWith
            if (result == null) {
                Iterator<String> iterator = n.getPropertyKeys().iterator();
                if (iterator.hasNext()) {
                    result = n.getProperty(iterator.next()).toString(); // get the first property
                }
            }
        }
        return result == null ? String.valueOf(n.getId()) : result; // if the node has no property return the ID
    }

    public static String filterCaption(Node n, Set<String> captions, BiPredicate<String, String> predicate) {

        for (String caption : captions) {
            for (String key : n.getPropertyKeys()) {
                if (predicate.test(key, caption))
                    return n.getProperty(key).toString();
            }

        }
        return null;
    }
}
