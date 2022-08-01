package apoc.test.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
public @interface EnvSetting {
    String key();
    String value();
}
