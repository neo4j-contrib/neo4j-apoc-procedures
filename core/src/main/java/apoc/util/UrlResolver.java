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
package apoc.util;

/**
 * @author mh
 * @since 29.05.16
 */
public class UrlResolver {
    private final String defaultScheme;
    private final String defaultHost;
    private final int defaultPort;
    private final String defaultUrl;

    public UrlResolver(String defaultScheme, String defaultHost, int defaultPort) {
        this.defaultScheme = defaultScheme;
        this.defaultHost = defaultHost;
        this.defaultPort = defaultPort;
        this.defaultUrl = defaultScheme + "://" + defaultHost + ":" + defaultPort;
    }

    public String getUrl(String prefix, String hostOrKey) {
        String url = getConfiguredUrl(prefix, hostOrKey);
        if (url != null) return url;
        url = getConfiguredUrl(prefix, "");
        if (url != null) return url;
        url = resolveHost(hostOrKey);
        return url == null ? defaultUrl : url;
    }

    public String getConfiguredUrl(String prefix, String key) {
        String url = Util.getLoadUrlByConfigFile(prefix, key, "url")
                .orElse(Util.getLoadUrlByConfigFile(prefix, key, "host")
                        .map(this::resolveHost)
                        .orElse(null));
        return url;
    }

    public String resolveHost(String host) {
        if (host != null) {
            if (host.contains("//")) return host;
            if (host.contains(":")) return defaultScheme + "://" + host;
            return defaultScheme + "://" + host + ":" + defaultPort;
        }
        return null;
    }
}
