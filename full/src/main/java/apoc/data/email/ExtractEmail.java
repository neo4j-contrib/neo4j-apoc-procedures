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

import static apoc.data.email.ExtractEmailHandler.extractEmail;

import apoc.Extended;
import apoc.util.MissingDependencyException;
import java.util.Map;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

@Extended
public class ExtractEmail {
    public static final String EMAIL_MISSING_DEPS_ERROR =
            "Cannot find the needed jar into the plugins folder in order to use . \n"
                    + "Please put the apoc-email-dependencies-5.x.x-all.jar into plugin folder.\n"
                    + "See the documentation: https://neo4j.com/labs/apoc/5/overview/apoc.data/apoc.data.email/#_install_dependencies";

    @UserFunction("apoc.data.email")
    @Description(
            "apoc.data.email('email_address') as {personal,user,domain} - extract the personal name, user and domain as a map")
    public Map<String, String> email(final @Name("email_address") String value) {
        try {
            return extractEmail(value);
        } catch (NoClassDefFoundError e) {
            throw new MissingDependencyException(EMAIL_MISSING_DEPS_ERROR);
        }
    }
}
