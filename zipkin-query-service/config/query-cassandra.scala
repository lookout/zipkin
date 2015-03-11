/*
 * Copyright 2012 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import com.twitter.zipkin.builder.QueryServiceBuilder
import com.twitter.zipkin.cassandra
import com.twitter.zipkin.storage.Store

// development mode.
// val keyspaceBuilder = cassandra.Keyspace.static()
val keyspaceBuilder = cassandra.Keyspace.static(nodes = Set("127.0.0.1"), username = "cassandra", password = "cassandra")

val storeBuilder = Store.Builder(
  cassandra.StorageBuilder(keyspaceBuilder),
  cassandra.IndexBuilder(keyspaceBuilder),
  cassandra.AggregatesBuilder(keyspaceBuilder))

QueryServiceBuilder(storeBuilder)
