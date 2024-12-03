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

public enum SupportedProtocolsExtended {
    http(true, null),
    https(true, null),
    ftp(true, null),
    s3(UtilExtended.classExists("com.amazonaws.services.s3.AmazonS3"), "apoc.util.s3.S3UrlStreamHandlerFactory"),
    gs(UtilExtended.classExists("com.google.cloud.storage.Storage"), "apoc.util.google.cloud.GCStorageURLStreamHandlerFactory"),
    hdfs(UtilExtended.classExists("org.apache.hadoop.fs.FileSystem"), "org.apache.hadoop.fs.FsUrlStreamHandlerFactory"),
    file(true, null);

    private final boolean enabled;

    private final String urlStreamHandlerClassName;

    SupportedProtocolsExtended(boolean enabled, String urlStreamHandlerClassName) {
        this.enabled = enabled;
        this.urlStreamHandlerClassName = urlStreamHandlerClassName;
    }

    public boolean isEnabled() {
        return enabled;
    }

    String getUrlStreamHandlerClassName() {
        return urlStreamHandlerClassName;
    }
}
