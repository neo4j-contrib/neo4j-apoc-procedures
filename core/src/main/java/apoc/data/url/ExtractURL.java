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
package apoc.data.url;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static apoc.util.Util.map;

/**
 * This class is pretty simple.  It just constructs a java.net.URL instance
 * from the user's input to do validation/parsing, and delegates the actual
 * functionality to that class.  As such, the behavior of these functions
 * matches that class, which is nice.
 */
public class ExtractURL {
    @UserFunction("apoc.data.url")
    @Description("apoc.data.url('url') as {protocol,host,port,path,query,file,anchor,user} | turn URL into map structure")
    public Map<String, Object> parse(final @Name("url") String value) {
        if (value == null) return null;
        try {
            URI u = new URI(value);
            Long port = u.getPort() == -1 ? null : (long) u.getPort();
            // if the scheme is not present, it's a bad URL
            if(u.getScheme()== null) {
                return null;
            }
            StringBuilder file = new StringBuilder(u.getPath());
            if(u.getQuery() != null){
                file.append("?").append(u.getQuery());
            }
            return map("protocol", u.getScheme(), "user", u.getUserInfo(), "host", u.getHost(), "port", port, "path", u.getPath(),"file", file.toString(), "query", u.getQuery(), "anchor", u.getFragment());
        } catch (URISyntaxException exc) {
            return null;
        }
    }
}
