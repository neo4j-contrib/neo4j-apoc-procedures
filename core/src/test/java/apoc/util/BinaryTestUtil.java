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
package apoc.util;

import org.apache.commons.io.FileUtils;
import java.io.File;
import java.nio.charset.Charset;

import static java.nio.charset.StandardCharsets.UTF_8;


public class BinaryTestUtil {

    public static byte[] fileToBinary(File file, String compression) {
        try {
            final String data = FileUtils.readFileToString(file, UTF_8);
            return compression.equals(CompressionAlgo.NONE.name())
                    ? data.getBytes(UTF_8)
                    : CompressionAlgo.valueOf(compression).compress(data, UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public static String readFileToString(File file, Charset charset, CompressionAlgo compression) {
        try {
            return compression.isNone() ?
                    TestUtil.readFileToString(file, charset)
                    : compression.decompress(FileUtils.readFileToByteArray(file), charset);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String getDecompressedData(CompressionAlgo algo, Object data) {
        try {
            return algo.isNone() ? (String) data : algo.decompress((byte[]) data, UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
}