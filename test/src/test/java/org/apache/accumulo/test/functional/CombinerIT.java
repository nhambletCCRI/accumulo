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
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner.Type;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.util.Admin;
import org.junit.Test;

public class CombinerIT extends MacTest {
  
  private void checkSum(Connector c) throws Exception {
    Scanner s = c.createScanner("test", Authorizations.EMPTY);
    Iterator<Entry<Key,Value>> i = s.iterator();
    assertTrue(i.hasNext());
    Entry<Key,Value> entry = i.next();
    assertEquals("45", entry.getValue().toString());
    assertFalse(i.hasNext());
  }
  
  @Test(timeout=60*1000)
  public void aggregationTest() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("test");
    IteratorSetting setting = new IteratorSetting(10, SummingCombiner.class);
    SummingCombiner.setEncodingType(setting, Type.STRING);
    SummingCombiner.setColumns(setting, Collections.singletonList(new IteratorSetting.Column("cf")));
    c.tableOperations().attachIterator("test", setting);
    BatchWriter bw = c.createBatchWriter("test", new BatchWriterConfig());
    for (int i = 0; i < 10; i++) {
      Mutation m = new Mutation("row1");
      m.put("cf".getBytes(), "col1".getBytes(), ("" + i).getBytes());
      bw.addMutation(m);
    }
    bw.close();
    checkSum(c);
    assertEquals(0, cluster.exec(Admin.class, "stopAll").waitFor());
    cluster.stop();
    cluster.start();
    checkSum(c);
  }
  
}
