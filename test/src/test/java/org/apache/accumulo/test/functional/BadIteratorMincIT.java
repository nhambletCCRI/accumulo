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

import java.util.EnumSet;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class BadIteratorMincIT extends MacTest {
  
  @Test(timeout=60*1000)
  public void test() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("foo");
    IteratorSetting is = new IteratorSetting(30, BadIterator.class);
    c.tableOperations().attachIterator("foo", is, EnumSet.of(IteratorScope.minc));
    BatchWriter bw = c.createBatchWriter("foo", new BatchWriterConfig());
    
    Mutation m = new Mutation(new Text("r1"));
    m.put(new Text("acf"), new Text("foo"), new Value("1".getBytes()));
    bw.addMutation(m);
    bw.close();
    
    c.tableOperations().flush("foo", null, null, false);
    UtilWaitThread.sleep(1000);
    
    // minc should fail, so there should be no files
    FunctionalTestUtils.checkRFiles(c, "foo", 1, 1, 0, 0);
    
    // try to scan table
    Scanner scanner = c.createScanner("foo", Authorizations.EMPTY);
    
    int count = 0;
    for (@SuppressWarnings("unused")
    Entry<Key,Value> entry : scanner) {
      count++;
    }
    
    if (count != 1)
      throw new Exception("Did not see expected # entries " + count);
    
    // remove the bad iterator
    c.tableOperations().removeIterator("foo", BadIterator.class.getSimpleName(), EnumSet.of(IteratorScope.minc));
    
    UtilWaitThread.sleep(5000);
    
    // minc should complete
    FunctionalTestUtils.checkRFiles(c, "foo", 1, 1, 1, 1);
    
    count = 0;
    for (@SuppressWarnings("unused")
    Entry<Key,Value> entry : scanner) {
      count++;
    }
    
    if (count != 1)
      throw new Exception("Did not see expected # entries " + count);
    
    // now try putting bad iterator back and deleting the table
    c.tableOperations().attachIterator("foo", is, EnumSet.of(IteratorScope.minc));
    bw = c.createBatchWriter("foo", new BatchWriterConfig());
    m = new Mutation(new Text("r2"));
    m.put(new Text("acf"), new Text("foo"), new Value("1".getBytes()));
    bw.addMutation(m);
    bw.close();
    
    // make sure property is given time to propagate
    UtilWaitThread.sleep(500);
    
    c.tableOperations().flush("foo", null, null, false);
    
    // make sure the flush has time to start
    UtilWaitThread.sleep(1000);
    
    // this should not hang
    c.tableOperations().delete("foo");
  }
  


  
}
