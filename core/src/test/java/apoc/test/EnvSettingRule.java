package apoc.test;

import apoc.test.annotations.Env;
import apoc.test.annotations.EnvSetting;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.lang.reflect.Method;

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
