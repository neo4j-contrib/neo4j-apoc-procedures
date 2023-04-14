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
package apoc.log;

import apoc.ApocConfig;
import apoc.Extended;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.function.Consumer;

/**
 * @author bradnussbaum
 * @since 2017.07.28
 */
@Extended
public class Logging {

    @Context
    public Log log;

    @Context
    public ApocConfig apocConfig;

    @Procedure
    @Description("apoc.log.error(message, params) - logs error message")
    public void error(@Name("message") String message,
                      @Name(value = "params", defaultValue = "[]") List<Object> params) {
        log((logMessage) -> log.error(logMessage), message, params);
    }

    @Procedure
    @Description("apoc.log.warn(message, params) - logs warn message")
    public void warn(@Name("message") String message,
                     @Name(value = "params", defaultValue = "[]") List<Object> params) {
        log((logMessage) -> log.warn(logMessage), message, params);
    }

    @Procedure
    @Description("apoc.log.info(message, params) - logs info message")
    public void info(@Name("message") String message,
                     @Name(value = "params", defaultValue = "[]") List<Object> params) {
        log((logMessage) -> log.info(logMessage), message, params);
    }

    @Procedure
    @Description("apoc.log.debug(message, params) - logs debug message")
    public void debug(@Name("message") String message,
                      @Name(value = "params", defaultValue = "[]") List<Object> params) {
        log((logMessage) -> log.debug(logMessage), message, params);
    }

    public String format(String message, List<Object> params) { // visible for testing
        if (canLog()) {
            String formattedMessage = String.format(message, params.isEmpty() ? new Object[0] : params.toArray(new Object[params.size()]));
            if (ApocConfig.LoggingType.safe == apocConfig.getLoggingType()) {
                return formattedMessage.replaceAll("\\.| |\\t", "_").toLowerCase();
            }
            return formattedMessage;
        }
        return null;
    }

    private void log(Consumer<String> consumer, String message, List<Object> params) {
        String format = format(message, params);
        if (format != null) {
            consumer.accept(format);
        }
    }

    private boolean canLog() {
        if (ApocConfig.LoggingType.none == apocConfig.getLoggingType()) {
            return false;
        }
        return apocConfig.getRateLimiter().canExecute();
    }

}
