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
package apoc.monitor;

import apoc.date.Date;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.text.SimpleDateFormat;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.*;

public class KernelProcedureTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setup() {
        TestUtil.registerProcedure(db, Kernel.class);
    }

    private SimpleDateFormat format = new SimpleDateFormat(Date.DEFAULT_FORMAT);

    @Test
    public void testGetKernelInfo() {
        long now = System.currentTimeMillis();
        testCall(db, "CALL apoc.monitor.kernel()", (row) -> {
            try {
                String startTime = (String) row.get("kernelStartTime");
                String kernelVersion = String.valueOf(row.get("kernelVersion"));
                assertEquals("neo4j", row.get("databaseName"));
                assertTrue(format.parse(startTime).getTime() < now);
                assertTrue(kernelVersion.contains("4."));
                assertNotNull(row.get("storeLogVersion"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
