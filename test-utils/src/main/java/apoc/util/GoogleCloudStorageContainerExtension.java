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

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

import static java.net.HttpURLConnection.HTTP_OK;

public class GoogleCloudStorageContainerExtension extends GenericContainer<GoogleCloudStorageContainerExtension> {

    public GoogleCloudStorageContainerExtension() {
        super("fsouza/fake-gcs-server:latest");
        this.withCommand("-scheme http");

        setWaitStrategy(new HttpWaitStrategy()
                .forPath("/storage/v1/b")
                .forPort(4443)
                .forStatusCodeMatching(response -> response == HTTP_OK));

        addExposedPort(4443);
    }

    public GoogleCloudStorageContainerExtension withMountedResourceFile(String resourceFilePath, String gcsPath) {
        this.withClasspathResourceMapping(resourceFilePath, "/data" + gcsPath, BindMode.READ_ONLY);
        return this;
    }
}