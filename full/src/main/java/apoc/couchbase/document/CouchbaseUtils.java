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
package apoc.couchbase.document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.java.json.JsonObject;

/**
 * Utility class for Couchbase procedures.
 *
 * @since 15.8.2016
 * @author inserpio
 */
public class CouchbaseUtils {

  /**
   * Converts a {@link MutationToken} into a {@link Map} so that it can be
   * {@link Stream}-ed and returned by the procedures
   *
   * @param mutationToken
   *          the mutation token to convert
   * @return the converted mutation token in the form of a Map
   */
  public static Map<String, Object> convertMutationTokenToMap(MutationToken mutationToken) {
    Map<String, Object> result = null;
    if (mutationToken != null) {
      result = new HashMap<String, Object>();
      result.put("vbucketID", mutationToken.partitionID());
      result.put("vbucketUUID", mutationToken.partitionUUID());
      result.put("sequenceNumber", mutationToken.sequenceNumber());
      result.put("bucket", mutationToken.bucketName());
    }
    return result;
  }

  /**
   * Converts a {@link JsonObject} list into a {@link CouchbaseQueryResult} so that it can
   * be {@link Stream}-ed and returned by the procedures
   *
   * @param jsonObjects
   *          the {@link JsonObject} list to convert
   * @return the converted list in the form of a CouchbaseQueryResult
   */
  public static CouchbaseQueryResult convertToCouchbaseQueryResult(List<JsonObject> jsonObjects) {
    CouchbaseQueryResult result = null;
    if (jsonObjects != null && jsonObjects.size() > 0) {
      List<Map<String, Object>> list = new ArrayList<Map<String, Object>>(jsonObjects.size());
      for (JsonObject jsonObject : jsonObjects) {
        list.add(jsonObject.toMap());
      }
      result = new CouchbaseQueryResult(list);
    }
    return result;
  }
}
