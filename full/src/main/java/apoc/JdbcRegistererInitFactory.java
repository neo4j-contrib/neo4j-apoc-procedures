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
package apoc;

import apoc.load.Jdbc;
import apoc.util.Util;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

@ServiceProvider
public class JdbcRegistererInitFactory extends ExtensionFactory<JdbcRegistererInitFactory.Dependencies> {

    public interface Dependencies {
        ApocConfig apocConfig();
    }

    public JdbcRegistererInitFactory() {
        super(ExtensionType.GLOBAL, "JdbcDriverRegisterer");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        return new LifecycleAdapter() {
            @Override
            public void init() throws Exception {
                // we need to await initialization of ApocConfig. Unfortunately Neo4j's internal service loading tooling does *not* honor the order of service loader META-INF/services files.
                Util.newDaemonThread(() -> {
                    ApocConfig apocConfig = dependencies.apocConfig();
                    while (!apocConfig.isInitialized()) {
                        Util.sleep(10);
                    }
                    Iterators.stream(apocConfig.getKeys("apoc.jdbc"))
                            .filter(k -> k.endsWith("driver"))
                            .forEach(k -> Jdbc.loadDriver(k));
                }).start();
            }
        };
    }

}
