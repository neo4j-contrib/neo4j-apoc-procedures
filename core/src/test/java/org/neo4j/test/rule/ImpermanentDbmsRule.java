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
package org.neo4j.test.rule;

import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

/**
 * JUnit @Rule for configuring, creating and managing an ImpermanentGraphDatabase instance.
 */
@Deprecated
public class ImpermanentDbmsRule extends DbmsRule
{
    private final LogProvider userLogProvider;
    private final LogProvider internalLogProvider;

    public ImpermanentDbmsRule()
    {
        this( null );
    }

    public ImpermanentDbmsRule( LogProvider logProvider )
    {
        this.userLogProvider = logProvider;
        this.internalLogProvider = logProvider;
    }

    @Override
    public ImpermanentDbmsRule startLazily()
    {
        return (ImpermanentDbmsRule) super.startLazily();
    }

    @Override
    protected DatabaseManagementServiceBuilder newFactory()
    {
        return maybeSetInternalLogProvider( maybeSetUserLogProvider( new TestDatabaseManagementServiceBuilder().impermanent() ) );
    }

    protected final TestDatabaseManagementServiceBuilder maybeSetUserLogProvider( TestDatabaseManagementServiceBuilder factory )
    {
        return ( userLogProvider == null ) ? factory : factory.setUserLogProvider( userLogProvider );
    }

    protected final TestDatabaseManagementServiceBuilder maybeSetInternalLogProvider( TestDatabaseManagementServiceBuilder factory )
    {
        return ( internalLogProvider == null ) ? factory : factory.setInternalLogProvider( internalLogProvider );
    }
}
