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
package apoc.util.hdfs;

import apoc.util.StreamConnection;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSUtils {

    private HDFSUtils() {}

    public static StreamConnection readFile(String fileName) throws IOException {
        FileSystem hdfs = getFileSystem(fileName);
        Path file = getPath(fileName);
        FileStatus fileStatus = hdfs.getFileStatus(file);
        return new StreamConnection() {
            @Override
            public InputStream getInputStream() throws IOException {
                return hdfs.open(file);
            }

            @Override
            public String getEncoding() {
                return "";
            }

            @Override
            public long getLength() {
                return fileStatus.getLen();
            }

            @Override
            public String getName() {
                return fileName;
            }
        };
    }

    public static StreamConnection readFile(URL url) throws IOException {
        return readFile(url.toString());
    }

    public static OutputStream writeFile(String fileName) throws IOException {
        FileSystem hdfs = getFileSystem(fileName);
        Path file = getPath(fileName);
        return hdfs.create(file);
    }

    public static Path getPath(String fileName) {
        return new Path(URI.create(fileName));
    }

    public static FileSystem getFileSystem(String fileName) throws IOException {
        Configuration configuration = new Configuration();
        return FileSystem.get(URI.create(fileName), configuration);
    }
}
