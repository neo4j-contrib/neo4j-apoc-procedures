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
package apoc.data;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.regex.Pattern;

/**
 * Extracts domains from URLs and email addresses
 * @deprecated use ExtractEmail or ExtractURI
 */
@Deprecated
public class Extract {

    public static final Pattern DOMAIN = Pattern.compile("([\\w-]+\\.[\\w-]+)+(\\w+)");

    @UserFunction
    @Description("apoc.data.domain('url_or_email_address') YIELD domain - extract the domain name from a url or an email address. If nothing was found, yield null.")
    public String domain(final @Name("url_or_email_address") String value) {
        if (value != null) {
            if (value.contains("@")) {
                String[] tokens = value.split("[@/<>]");
                for (int i = tokens.length - 1; i >= 0; i--) {
                    String token = tokens[i];
                    if (DOMAIN.matcher(token).matches()) return token;
                }
            } else {
                for (String part : value.split("[@/<>]")) {
                    if (DOMAIN.matcher(part).matches()) return part;
                }
            }
        }
        return null;
    }
}
