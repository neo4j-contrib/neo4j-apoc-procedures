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

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * a kernel extension for the TTL config
 */
@ServiceProvider
public class TTLConfigExtensionFactory extends ExtensionFactory<TTLConfigExtensionFactory.Dependencies> {

    public interface Dependencies {
        ApocConfig config();
        GlobalProcedures globalProceduresRegistry();
    }

    public TTLConfigExtensionFactory() {
        super(ExtensionType.DATABASE, "TTLConfig");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        return new TTLConfig(dependencies.config(), dependencies.globalProceduresRegistry());
    }

}
