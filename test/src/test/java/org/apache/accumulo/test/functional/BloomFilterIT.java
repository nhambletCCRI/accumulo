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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.keyfunctor.ColumnFamilyFunctor;
import org.apache.accumulo.core.file.keyfunctor.ColumnQualifierFunctor;
import org.apache.accumulo.core.file.keyfunctor.RowFunctor;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class BloomFilterIT extends MacTest {
  
  @Override
  public void configure(MiniAccumuloConfig cfg) {
    cfg.setDefaultMemory(500, MemoryUnit.MEGABYTE);
  }
  
  @Test(timeout=120*1000)
  public void test() throws Exception {
    Connector c = getConnector();
    for (String table : "bt1 bt2 bt3 bt4".split(" ")) {
      c.tableOperations().create(table);
    }
    write(c, "bt1", 1, 0, 1000000000, 500);
    write(c, "bt2", 2, 0, 1000000000, 500);
    write(c, "bt3", 3, 0, 1000000000, 500);
    
    // test inserting an empty key
    BatchWriter bw = c.createBatchWriter("bt4", new BatchWriterConfig());
    Mutation m = new Mutation(new Text(""));
    m.put(new Text(""), new Text(""), new Value("foo1".getBytes()));
    bw.addMutation(m);
    bw.close();
    c.tableOperations().flush("bt4", null, null, true);
    
    for (String table : new String[] {"bt1", "bt2", "bt3"}) {
      c.tableOperations().setProperty(table, Property.TABLE_INDEXCACHE_ENABLED.getKey(), "false");
      c.tableOperations().setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "false");
      c.tableOperations().compact(table, null, null, true, true);
    }
    
    // ensure compactions are finished
    FunctionalTestUtils.checkRFiles(c, "bt1", 1, 1, 1, 1);
    FunctionalTestUtils.checkRFiles(c, "bt2", 1, 1, 1, 1);
    FunctionalTestUtils.checkRFiles(c, "bt3", 1, 1, 1, 1);
    FunctionalTestUtils.checkRFiles(c, "bt4", 1, 1, 1, 1);
    
    // these queries should only run quickly if bloom filters are working, so lets get a base
    long t1 = query(c, "bt1", 1, 0, 1000000000, 100000, 500);
    long t2 = query(c, "bt2", 2, 0, 1000000000, 100000, 500);
    long t3 = query(c, "bt3", 3, 0, 1000000000, 100000, 500);
    
    c.tableOperations().setProperty("bt1", Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    c.tableOperations().setProperty("bt1", Property.TABLE_BLOOM_KEY_FUNCTOR.getKey(), RowFunctor.class.getName());
    c.tableOperations().compact("bt1", null, null, false, true);
    
    c.tableOperations().setProperty("bt2", Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    c.tableOperations().setProperty("bt2", Property.TABLE_BLOOM_KEY_FUNCTOR.getKey(), ColumnFamilyFunctor.class.getName());
    c.tableOperations().compact("bt2", null, null, false, true);
    
    c.tableOperations().setProperty("bt3", Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    c.tableOperations().setProperty("bt3", Property.TABLE_BLOOM_KEY_FUNCTOR.getKey(), ColumnQualifierFunctor.class.getName());
    c.tableOperations().compact("bt3", null, null, false, true);
    
    c.tableOperations().setProperty("bt4", Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    c.tableOperations().setProperty("bt4", Property.TABLE_BLOOM_KEY_FUNCTOR.getKey(), RowFunctor.class.getName());
    c.tableOperations().compact("bt4", null, null, false, true);
    
    // these queries should only run quickly if bloom
    // filters are working
    long tb1 = query(c, "bt1", 1, 0, 1000000000, 100000, 500);
    long tb2 = query(c, "bt2", 2, 0, 1000000000, 100000, 500);
    long tb3 = query(c, "bt3", 3, 0, 1000000000, 100000, 500);
    
    timeCheck(t1, tb1);
    timeCheck(t2, tb2);
    timeCheck(t3, tb3);
    
    // test querying for empty key
    Scanner scanner = c.createScanner("bt4", Authorizations.EMPTY);
    scanner.setRange(new Range(new Text("")));
    
    if (!scanner.iterator().next().getValue().toString().equals("foo1")) {
      throw new Exception("Did not see foo1");
    }
    
  }
  
  private void timeCheck(long t1, long t2) throws Exception {
    if (((t1 - t2) * 1.0 / t1) < .1) {
      throw new Exception("Queries had less than 10% improvement (old: " + t1 + " new: " + t2 + " improvement: " + ((t1 - t2) * 100. / t1) + "%)");
    }
  }
  
  private long query(Connector c, String table, int depth, long start, long end, int num, int step) throws Exception {
    Random r = new Random(42);
    
    HashSet<Long> expected = new HashSet<Long>();
    List<Range> ranges = new ArrayList<Range>(num);
    Text key = new Text();
    Text row = new Text("row"), cq = new Text("cq"), cf = new Text("cf");
    
    for (int i = 0; i < num; ++i) {
      Long k = ((r.nextLong() & 0x7fffffffffffffffl) % (end - start)) + start;
      key.set(String.format("k_%010d", k));
      Range range = null;
      Key acuKey;
      
      if (k % (start + step) == 0) {
        expected.add(k);
      }
      
      switch (depth) {
        case 1:
          range = new Range(new Text(key));
          break;
        case 2:
          acuKey = new Key(row, key, cq);
          range = new Range(acuKey, true, acuKey.followingKey(PartialKey.ROW_COLFAM), false);
          break;
        case 3:
          acuKey = new Key(row, cf, key);
          range = new Range(acuKey, true, acuKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false);
          break;
      }
      
      ranges.add(range);
    }
    
    BatchScanner bs = c.createBatchScanner(table, Authorizations.EMPTY, 3);
    bs.setRanges(ranges);
    
    long t1 = System.currentTimeMillis();
    
    for (Entry<Key,Value> entry : bs) {
      long v = Long.parseLong(entry.getValue().toString());
      if (!expected.remove(v)) {
        throw new Exception("Got unexpected return " + entry.getKey() + " " + entry.getValue());
      }
    }
    
    long t2 = System.currentTimeMillis();
    
    if (expected.size() > 0) {
      throw new Exception("Did not get all expected values " + expected.size());
    }
    
    bs.close();
    
    return t2 - t1;
  }
  
  private void write(Connector c, String table, int depth, long start, long end, int step) throws Exception {
    
    BatchWriter bw = c.createBatchWriter(table, new BatchWriterConfig());
    
    for (long i = start; i < end; i += step) {
      String key = String.format("k_%010d", i);
      
      Mutation m = null;
      
      switch (depth) {
        case 1:
          m = new Mutation(new Text(key));
          m.put(new Text("cf"), new Text("cq"), new Value(("" + i).getBytes()));
          break;
        case 2:
          m = new Mutation(new Text("row"));
          m.put(new Text(key), new Text("cq"), new Value(("" + i).getBytes()));
          break;
        case 3:
          m = new Mutation(new Text("row"));
          m.put(new Text("cf"), new Text(key), new Value(("" + i).getBytes()));
          break;
      }
      
      bw.addMutation(m);
    }
    
    bw.close();
    
    c.tableOperations().flush(table, null, null, true);
  }
}
