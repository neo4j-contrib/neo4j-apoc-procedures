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
package apoc.test;

import apoc.test.annotations.Env;
import apoc.test.annotations.EnvSetting;
import java.lang.reflect.Method;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class EnvSettingRule implements TestRule {

    private final EnvironmentVariables env = new EnvironmentVariables();
    private final RuleChain delegate = RuleChain.outerRule((base, description) -> new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    Class<?> testClass = description.getTestClass();
                    String methodName = description.getMethodName();
                    Method method = testClass.getMethod(methodName);
                    Env annotation = method.getAnnotation(Env.class);
                    for (EnvSetting envSetting : annotation.value()) {
                        env.set(envSetting.key(), envSetting.value());
                    }
                    base.evaluate();
                }
            })
            .around(env);

    @Override
    public Statement apply(Statement base, Description description) {
        return delegate.apply(base, description);
    }

    public RuleChain around(TestRule enclosedRule) {
        return delegate.around(enclosedRule);
    }
}
