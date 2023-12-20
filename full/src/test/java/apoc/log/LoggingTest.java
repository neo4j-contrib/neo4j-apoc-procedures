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

import static apoc.ApocConfig.apocConfig;
import static java.util.Arrays.asList;

import apoc.ApocConfig;
import apoc.util.SimpleRateLimiter;
import apoc.util.TestUtil;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

public class LoggingTest {

    public static AssertableLogProvider logProvider = new AssertableLogProvider();

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule(logProvider);

    @AfterClass
    public static void teardown() {
        db.shutdown();
    }

    @Test
    public void shouldWriteSafeStrings() {
        // given
        Logging logging = initLogging(ApocConfig.LoggingType.safe, 10000, 10);

        // when
        String stringWithWhitespaces = logging.format("Test %s ", asList(1));
        String stringWithWhitespacesAndTabs = logging.format("Test %s \t", asList(1));
        String stringWithWhitespacesAndTabsAndNewLine = logging.format("Test %s \t\nNewLine", asList(1));

        // then
        Assert.assertEquals("test_1_", stringWithWhitespaces);
        Assert.assertEquals("test_1__", stringWithWhitespacesAndTabs);
        Assert.assertEquals("test_1__\nnewline", stringWithWhitespacesAndTabsAndNewLine);
    }

    @NotNull
    private Logging initLogging(ApocConfig.LoggingType safe, int i, int i2) {
        apocConfig().setLoggingType(safe);
        apocConfig().setRateLimiter(new SimpleRateLimiter(i, i2));
        Logging logging = new Logging();
        logging.apocConfig = apocConfig();
        return logging;
    }

    @Test
    public void shouldWriteRawStrings() {
        // given
        Logging logging = initLogging(ApocConfig.LoggingType.raw, 10000, 10);

        // when
        String stringWithWhitespaces = logging.format("Test %s ", asList(1));
        String stringWithWhitespacesAndTabs = logging.format("Test %s \t", asList(1));
        String stringWithWhitespacesAndTabsAndNewLine = logging.format("Test %s \t\nNewLine", asList(1));

        // then
        Assert.assertEquals("Test 1 ", stringWithWhitespaces);
        Assert.assertEquals("Test 1 \t", stringWithWhitespacesAndTabs);
        Assert.assertEquals("Test 1 \t\nNewLine", stringWithWhitespacesAndTabsAndNewLine);
    }

    @Test
    public void shouldNotLog() {
        // given
        Logging logging = initLogging(ApocConfig.LoggingType.none, 10000, 10);

        // when
        String stringWithWhitespaces = logging.format("Test %s ", asList(1));
        String stringWithWhitespacesAndTabs = logging.format("Test %s \t", asList(1));
        String stringWithWhitespacesAndTabsAndNewLine = logging.format("Test %s \t\nNewLine", asList(1));

        // then
        Assert.assertNull(stringWithWhitespaces);
        Assert.assertNull(stringWithWhitespacesAndTabs);
        Assert.assertNull(stringWithWhitespacesAndTabsAndNewLine);
    }

    @Test
    public void shouldSkipMessagesFor10Seconds() {
        // given
        Logging logging = initLogging(ApocConfig.LoggingType.safe, 10000, 10);

        List<String> all =
                IntStream.range(0, 10).mapToObj(i -> "test_" + i + "_").collect(Collectors.toList());

        all.addAll(IntStream.range(50, 60).mapToObj(i -> "test_" + i + "_").collect(Collectors.toList()));

        // when
        IntStream.range(0, 100).forEach(i -> {
            if (i < 10) {
                // when
                String msg = logging.format("Test %s ", asList(i));

                // then
                Assert.assertEquals("test_" + i + "_", msg);
                all.remove(msg);
                return;
            }
            if (i >= 10 && i < 50) {
                // when
                String msg = logging.format("Test %s ", asList(i));

                // then
                Assert.assertNull(msg);
                return;
            }
            if (i == 50) {
                try {
                    Thread.sleep(11_000L); // wait for a new time window
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (i >= 50 && i < 60) {
                // when
                String msg = logging.format("Test %s ", asList(i));

                // then
                Assert.assertEquals("test_" + i + "_", msg);
                all.remove(msg);
                return;
            }
            if (i >= 60) {
                // when
                String msg = logging.format("Test %s ", asList(i));

                // then
                Assert.assertNull(msg);
            }
        });

        // then
        Assert.assertTrue(all.isEmpty());
    }

    @Test
    public void shouldWrite20MessagesIn1Second() {
        // given
        Logging logging = initLogging(ApocConfig.LoggingType.safe, 1000, 20);

        List<String> all =
                IntStream.range(0, 20).mapToObj(i -> "test_" + i + "_").collect(Collectors.toList());

        all.addAll(IntStream.range(50, 70).mapToObj(i -> "test_" + i + "_").collect(Collectors.toList()));

        // when
        IntStream.range(0, 100).forEach(i -> {
            if (i < 20) {
                // when
                String msg = logging.format("Test %s ", asList(i));

                // then
                Assert.assertEquals("test_" + i + "_", msg);
                all.remove(msg);
                return;
            }
            if (i >= 20 && i < 50) {
                // when
                String msg = logging.format("Test %s ", asList(i));

                // then
                Assert.assertNull(msg); // then
                return;
            }
            if (i == 50) {
                try {
                    Thread.sleep(2_000L); // wait for a new time window
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (i >= 50 && i < 70) {
                // when
                String msg = logging.format("Test %s ", asList(i));

                // then
                Assert.assertEquals("test_" + i + "_", msg);
                all.remove(msg);
                return;
            }
            if (i >= 70) {
                // when
                String msg = logging.format("Test %s ", asList(i));

                // then
                Assert.assertNull(msg);
            }
        });

        // then
        Assert.assertTrue(all.isEmpty());
    }

    @Test
    public void shouldCallTheProcedure() throws KernelException {
        // given
        TestUtil.registerProcedure(db, Logging.class);

        // when
        db.executeTransactionally("CALL apoc.log.warn('Prova %s', [1])");

        // then
        logProvider.print(System.out);
        //        logProvider.assertExactly(new
        // LogMatcherBuilder(Matchers.equalTo("org.neo4j.kernel.api.procedure.GlobalProcedures")).warn("prova_"));
    }
}
