/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.export.csv;

import apoc.meta.Meta;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

public class CsvHeaderField {

    private final int index;
    private final String name;
    private final String type;
    private final boolean array;
    private final String idSpace;
    private final Map<String, Object> optionalData;

    private CsvHeaderField(int index, String name, String type, boolean array, String idSpace, Map<String, Object> optionalData) {
        super();
        this.index = index;
        this.name = name;
        this.type = type;
        this.array = array;
        this.idSpace = idSpace;
        this.optionalData = optionalData;
    }

    public int getIndex() {
        return index;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public boolean isArray() {
        return array;
    }

    public String getIdSpace() {
        return idSpace;
    }

    public Map<String, Object> getOptionalData() {
        return optionalData;
    }

    public boolean isId() {
        return CsvLoaderConstants.ID_FIELD.equals(type) ||
                CsvLoaderConstants.START_ID_FIELD.equals(type) ||
                CsvLoaderConstants.END_ID_FIELD.equals(type);
    }

    public boolean isMeta() {
        return CsvLoaderConstants.LABEL_FIELD.equals(type) || CsvLoaderConstants.TYPE_FIELD.equals(type);
    }

    public boolean isIgnore() {
        return CsvLoaderConstants.IGNORE_FIELD.equals(type);
    }

    public static CsvHeaderField parse(final int index, final String attribute, final char quotationCharacter) {
        final String attributeCleaned = attribute.replaceAll(String.valueOf(quotationCharacter), "");

        final Matcher matcher = CsvLoaderConstants.FIELD_PATTERN.matcher(attributeCleaned);
        matcher.find();

        final String rawName = extractGroup(matcher, "name");
        final String typeExtracted = extractGroup(matcher, "type");
        String optParExtracted = extractGroup(matcher, "optPar");
        
        Map<String, Object> optionalData = new HashMap<>();
        
        if (optParExtracted != null) {
            optParExtracted = optParExtracted.replace("{", "").replace("}", "");
            Matcher matcherKeyValue = CsvLoaderConstants.KEY_VALUE_PATTERN.matcher(optParExtracted);
            while (matcherKeyValue.find()) {
                String key = matcherKeyValue.group("key");
                String value = matcherKeyValue.group("value");
                optionalData.put(key, value);
            }
        }
        
        final String type = (typeExtracted != null) ? typeExtracted : Meta.Types.STRING.name();
        final String name = nameAndTypeToAttribute(rawName, type);

        // we use a default, global id space if the id space is not defined explicitly
        final String idSpaceExtracted = extractGroup(matcher, "idspace");
        final String idSpace = (idSpaceExtracted != null) ? idSpaceExtracted : CsvLoaderConstants.DEFAULT_IDSPACE;

        // arrays are denoted with '[]'.
        // additionally, the ':LABEL' header type always denotes an array
        boolean array =
                CsvLoaderConstants.ARRAY_PATTERN.equals(extractGroup(matcher, "array")) ||
                "LABEL".equals(type);

        return new CsvHeaderField(index, name, type, array, idSpace, optionalData);
    }

    private static String nameAndTypeToAttribute(String name, String type) {
        switch (type) {
            case CsvLoaderConstants.ID_FIELD:
                if (name.isEmpty()) {
                    return CsvLoaderConstants.ID_ATTR;
                } else {
                    return name;
                }
            case CsvLoaderConstants.START_ID_FIELD:
                return CsvLoaderConstants.START_ID_ATTR;
            case CsvLoaderConstants.END_ID_FIELD:
                return CsvLoaderConstants.END_ID_ATTR;
            case CsvLoaderConstants.LABEL_FIELD:
                return CsvLoaderConstants.LABEL_ATTR;
            case CsvLoaderConstants.TYPE_FIELD:
                return CsvLoaderConstants.TYPE_ATTR;
        }
        return name;
    }

    private static String extractGroup(Matcher matcher, String groupName) {
        try {
            return matcher.group(groupName);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

}
