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

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * A Simple HTTP Handler to serve resource files to be used in testing.
 * This allows us to not rely on Github or other websites being up 100% of the time
 * which would result in flaky tests.
 */
public class SimpleHttpHandler implements HttpHandler {

    private static final Map<String, String> MIME_MAP = new HashMap<>();

    static {
        MIME_MAP.put("html", "text/html");
        MIME_MAP.put("json", "application/json");
        MIME_MAP.put("xml", "application/xml");
        MIME_MAP.put("zip", "application/zip");
        MIME_MAP.put("tgz", "application/x-gzip");
        MIME_MAP.put("gz", "application/x-gzip");
        MIME_MAP.put("txt", "text/plain");
    }

    public void handle(HttpExchange t) throws IOException {
        URI uri = t.getRequestURI();
        String path = uri.getPath().substring(1);

        File file;
        try {
            file = new File(Thread.currentThread()
                    .getContextClassLoader()
                    .getResource(path)
                    .toURI());
        } catch (Exception e) {
            // Object does not exist or is not a file: reject with 404 error.
            String response = "404 (Not Found)\n";
            t.sendResponseHeaders(404, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
            return;
        }

        // Object exists and is a file: accept with response code 200.
        String mime = lookupMime(path);

        Headers h = t.getResponseHeaders();
        h.set("Content-Type", mime);
        t.sendResponseHeaders(200, 0);

        OutputStream os = t.getResponseBody();
        FileInputStream fs = new FileInputStream(file);
        byte[] buffer = new byte[1024];
        int count;
        while ((count = fs.read(buffer)) != -1) {
            os.write(buffer, 0, count);
        }
        fs.close();
        os.close();
    }

    private static String getExt(String path) {
        int slashIndex = path.lastIndexOf('/');
        String basename = (slashIndex < 0) ? path : path.substring(slashIndex + 1);

        int dotIndex = basename.lastIndexOf('.');
        if (dotIndex >= 0) {
            return basename.substring(dotIndex + 1);
        } else {
            return "";
        }
    }

    private static String lookupMime(String path) {
        String ext = getExt(path).toLowerCase();
        return MIME_MAP.getOrDefault(ext, "application/octet-stream");
    }
}
