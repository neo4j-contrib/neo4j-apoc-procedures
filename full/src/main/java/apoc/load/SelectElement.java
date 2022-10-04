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
package apoc.load;


import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static apoc.load.LoadHtml.getElements;

public class SelectElement implements HtmlResultInterface {

    @Override
    public List<Map<String, Object>> getResult(Document document, String selector, LoadHtmlConfig config, List<String> errorList, Log log, AtomicInteger rows, Transaction tx) {
        final Elements select = document.select(selector);
        return getElements(select, config, errorList, log, rows, tx);
    }
}