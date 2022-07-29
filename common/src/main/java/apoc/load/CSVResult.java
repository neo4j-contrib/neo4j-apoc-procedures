package apoc.load;

import apoc.load.util.Results;

import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class CSVResult {
    public long lineNo;
    public List<Object> list;
    public List<String> strings;
    public Map<String, Object> map;
    public Map<String, String> stringMap;

    public CSVResult(String[] header, String[] list, long lineNo, boolean ignore, Map<String, Mapping> mapping, List<String> nullValues, EnumSet<Results> results) {
        this.lineNo = lineNo;
        removeNullValues(list, nullValues);

        this.strings = results.contains(Results.strings) ?
                (List)createList(header, list, ignore, mapping, false) : emptyList();
        this.stringMap = results.contains(Results.stringMap) ?
                (Map)createMap(header, list, ignore, mapping,false) : emptyMap();
        this.map = results.contains(Results.map) ?
                createMap(header, list, ignore, mapping,true) : emptyMap();
        this.list = results.contains(Results.list) ?
                    createList(header, list, ignore, mapping, true) : emptyList();
    }

    public void removeNullValues(String[] list, List<String> nullValues) {
        if (nullValues.isEmpty()) return;
        for (int i = 0; i < list.length; i++) {
            if (nullValues.contains(list[i]))list[i] = null;
        }
    }

    private List<Object> createList(String[] header, String[] list, boolean ignore, Map<String, Mapping> mappings, boolean convert) {
        if (!ignore && mappings.isEmpty()) return asList((Object[]) list);
        ArrayList<Object> result = new ArrayList<>(list.length);
        for (int i = 0; i < header.length; i++) {
            String name = header[i];
            if (name == null) continue;
            Mapping mapping = mappings.get(name);
            if (mapping != null) {
                if (mapping.ignore) continue;
                result.add(convert ? mapping.convert(list[i]) : list[i]);
            } else {
                result.add(list[i]);
            }
        }
        return result;
    }

    private Map<String, Object> createMap(String[] header, String[] list, boolean ignore, Map<String, Mapping> mappings, boolean convert) {
        if (header == null) return null;
        Map<String, Object> map = new LinkedHashMap<>(header.length, 1f);
        for (int i = 0; i < header.length; i++) {
            String name = header[i];
            if (ignore && name == null) continue;
            Mapping mapping = mappings.get(name);
            if (mapping == null) {
                map.put(name, list[i]);
            } else {
                if (mapping.ignore) continue;
                map.put(mapping.name, convert ? mapping.convert(list[i]) : list[i]);
            }
        }
        return map;
    }
}
