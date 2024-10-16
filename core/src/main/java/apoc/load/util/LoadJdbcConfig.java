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
package apoc.load.util;

import apoc.util.Util;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * @author ab-Larus
 * @since 03-10-18
 */
public class LoadJdbcConfig {

    private ZoneId zoneId = null;

    private Credentials credentials;

    private final Long fetchSize;

    private final boolean autoCommit;

    public LoadJdbcConfig(Map<String, Object> config) {
        config = config != null ? config : Collections.emptyMap();
        try {
            this.zoneId = config.containsKey("timezone")
                    ? ZoneId.of(config.get("timezone").toString())
                    : null;
        } catch (DateTimeException e) {
            throw new IllegalArgumentException(
                    String.format("The timezone field contains an error: %s", e.getMessage()));
        }
        this.credentials = config.containsKey("credentials")
                ? createCredentials((Map<String, String>) config.get("credentials"))
                : null;
        this.fetchSize = Util.toLong(config.getOrDefault("fetchSize", 5000L));
        this.autoCommit = Util.toBoolean(config.getOrDefault("autoCommit", false));
    }

    public ZoneId getZoneId() {
        return this.zoneId;
    }

    public Credentials getCredentials() {
        return this.credentials;
    }

    public static Credentials createCredentials(Map<String, String> credentials) {
        if (!credentials.getOrDefault("user", StringUtils.EMPTY).equals(StringUtils.EMPTY)
                && !credentials.getOrDefault("password", StringUtils.EMPTY).equals(StringUtils.EMPTY)) {
            return new Credentials(credentials.get("user"), credentials.get("password"));
        } else {
            throw new IllegalArgumentException("In config param credentials must be passed both user and password.");
        }
    }

    public static class Credentials {
        private String user;

        private String password;

        public Credentials(String user, String password) {
            this.user = user;

            this.password = password;
        }

        public String getUser() {
            return user;
        }

        public String getPassword() {
            return password;
        }
    }

    public boolean hasCredentials() {
        return this.credentials != null;
    }

    public Long getFetchSize() {
        return fetchSize;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }
}
