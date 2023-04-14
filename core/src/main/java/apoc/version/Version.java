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
package apoc.version;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.UserFunction;

/**
 * @author AgileLARUS
 * @since 24-07-17
 */

public class Version {

    @UserFunction("apoc.version")
    @Description("RETURN apoc.version() | return the current APOC installed version")
    public String version() {
        return Version.class.getPackage().getImplementationVersion();
    }
}
