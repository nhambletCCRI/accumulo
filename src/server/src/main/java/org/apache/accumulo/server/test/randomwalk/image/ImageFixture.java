/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.server.test.randomwalk.image;

import java.net.InetAddress;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.server.test.randomwalk.Fixture;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.hadoop.io.Text;

public class ImageFixture extends Fixture {
  
  String imageTableName;
  String indexTableName;
  
  @Override
  public void setUp(State state) throws Exception {
    
    Connector conn = state.getConnector();
    Instance instance = state.getInstance();
    
    SortedSet<Text> splits = new TreeSet<Text>();
    for (int i = 1; i < 256; i++) {
      splits.add(new Text(String.format("%04x", i << 8)));
    }
    
    String hostname = InetAddress.getLocalHost().getHostName().replaceAll("[-.]", "_");
    String pid = state.getPid();
    
    imageTableName = String.format("img_%s_%s_%d", hostname, pid, System.currentTimeMillis());
    state.set("imageTableName", imageTableName);
    
    indexTableName = String.format("img_ndx_%s_%s_%d", hostname, pid, System.currentTimeMillis());
    state.set("indexTableName", indexTableName);
    
    try {
      conn.tableOperations().create(imageTableName);
      conn.tableOperations().addSplits(imageTableName, splits);
      log.debug("Created table " + imageTableName + " (id:" + Tables.getNameToIdMap(instance).get(imageTableName) + ")");
    } catch (TableExistsException e) {
      log.error("Table " + imageTableName + " already exists.");
      throw e;
    }
    
    try {
      conn.tableOperations().create(indexTableName);
      log.debug("Created table " + indexTableName + " (id:" + Tables.getNameToIdMap(instance).get(indexTableName) + ")");
    } catch (TableExistsException e) {
      log.error("Table " + imageTableName + " already exists.");
      throw e;
    }
    
    state.set("numWrites", new Integer(0));
    state.set("totalWrites", new Integer(0));
    state.set("verified", new Integer(0));
    state.set("lastIndexRow", new Text(""));
  }
  
  @Override
  public void tearDown(State state) throws Exception {
    
    log.debug("Dropping tables: " + imageTableName + " " + indexTableName);
    
    Connector conn = state.getConnector();
    
    conn.tableOperations().delete(imageTableName);
    conn.tableOperations().delete(indexTableName);
    
    log.debug("Final total of writes: " + state.getInteger("totalWrites"));
  }
}
