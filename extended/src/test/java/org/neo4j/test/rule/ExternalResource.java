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

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * A better version of {@link org.junit.rules.ExternalResource} that properly handles exceptions in {@link
 * #after()}.
 */
public abstract class ExternalResource implements TestRule {
    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                before();
                Throwable failure = null;
                try {
                    base.evaluate();
                } catch (Throwable e) {
                    failure = e;
                } finally {
                    try {
                        after();
                    } catch (Throwable e) {
                        if (failure != null) {
                            failure.addSuppressed(e);
                        } else {
                            failure = e;
                        }
                    }
                }
                if (failure != null) {
                    throw failure;
                }
            }
        };
    }

    /**
     * Override to set up your specific external resource.
     */
    protected void before() {
        // do nothing
    }

    /**
     * Override to tear down your specific external resource.
     */
    protected void after() {
        // do nothing
    }
}