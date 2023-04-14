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
package apoc.date;

import apoc.Extended;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static apoc.date.Date.unit;


/**
 * @author tkroman
 * @since 9.04.2016
 */
@Extended
public class DateExpiry {
	@Procedure(mode = Mode.WRITE, deprecatedBy = "apoc.ttl.expire")
	@Description("CALL apoc.date.expire(node,time,'time-unit') - expire node at specified time by setting :TTL label and `ttl` property")
	@Deprecated
	public void expire(@Name("node") Node node, @Name("time") long time, @Name("timeUnit") String timeUnit) {
		node.addLabel(Label.label("TTL"));
		node.setProperty("ttl",unit(timeUnit).toMillis(time));
	}

	@Procedure(mode = Mode.WRITE, deprecatedBy = "apoc.ttl.expireIn")
	@Description("CALL apoc.date.expireIn(node,time,'time-unit') - expire node after specified length of time time by setting :TTL label and `ttl` property")
	@Deprecated
	public void expireIn(@Name("node") Node node, @Name("timeDelta") long time, @Name("timeUnit") String timeUnit) {
		node.addLabel(Label.label("TTL"));
		node.setProperty("ttl",System.currentTimeMillis() + unit(timeUnit).toMillis(time));
	}
}
