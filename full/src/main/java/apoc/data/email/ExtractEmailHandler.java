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
package apoc.data.email;

import apoc.util.Util;
import java.util.Map;
import javax.mail.internet.*;

/**
 * Separated class in order to throw MissingDependencyException if `javax.mail` is not present
 */
public class ExtractEmailHandler {
    public static Map<String, String> extractEmail(String value) {
        if (value == null || value.indexOf('@') == -1) {
            return null;
        }
        try {
            InternetAddress addr = new InternetAddress(value);
            String rawAddr = addr.getAddress();
            int idx = rawAddr.indexOf('@');

            return Util.map(
                    "personal",
                    addr.getPersonal(),
                    "user",
                    rawAddr.substring(0, idx),
                    "domain",
                    rawAddr.substring(idx + 1));
        } catch (AddressException adr) {
            return null;
        }
    }
}
