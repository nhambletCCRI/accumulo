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
package org.apache.accumulo.core.iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.DefaultIteratorEnvironment;
import org.apache.accumulo.core.iterators.IntersectingIterator;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.MultiIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class IntersectingIteratorTest extends TestCase {
  
  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();
  private static final Logger log = Logger.getLogger(IntersectingIterator.class);
  private static IteratorEnvironment env = new DefaultIteratorEnvironment();
  
  TreeMap<Key,Value> map;
  HashSet<Text> docs = new HashSet<Text>();
  Text[] columnFamilies;
  Text[] negatedColumns;
  Text[] otherColumnFamilies;
  Text[] searchFamilies;
  boolean[] notFlags;
  
  static int docid = 0;
  
  static {
    log.setLevel(Level.OFF);
  }
  
  static float hitRatio = 0.1f;
  
  private synchronized static TreeMap<Key,Value> createSortedMap(int numRows, int numDocsPerRow, Text[] columnFamilies, Text[] otherColumnFamilies,
      HashSet<Text> docs, Text[] negatedColumns) {
    Random r = new Random();
    Value v = new Value(new byte[0]);
    TreeMap<Key,Value> map = new TreeMap<Key,Value>();
    boolean[] negateMask = new boolean[columnFamilies.length];
    
    for (int i = 0; i < columnFamilies.length; i++) {
      negateMask[i] = false;
      if (negatedColumns.length > 0)
        for (Text ng : negatedColumns)
          if (columnFamilies[i].equals(ng))
            negateMask[i] = true;
    }
    for (int i = 0; i < numRows; i++) {
      Text row = new Text(String.format("%06d", i));
      for (int startDocID = docid; docid - startDocID < numDocsPerRow; docid++) {
        boolean docHits = true;
        Text doc = new Text(String.format("%010d", docid));
        for (int j = 0; j < columnFamilies.length; j++) {
          if (r.nextFloat() < hitRatio) {
            Key k = new Key(row, columnFamilies[j], doc);
            map.put(k, v);
            if (negateMask[j])
              docHits = false;
          } else {
            if (!negateMask[j])
              docHits = false;
          }
        }
        if (docHits) {
          docs.add(doc);
        }
        for (Text cf : otherColumnFamilies) {
          if (r.nextFloat() < hitRatio) {
            Key k = new Key(row, cf, doc);
            map.put(k, v);
          }
        }
      }
    }
    return map;
  }
  
  private synchronized static SortedKeyValueIterator<Key,Value> createIteratorStack(int numRows, int numDocsPerRow, Text[] columnFamilies,
      Text[] otherColumnFamilies, HashSet<Text> docs) throws IOException {
    Text nullText[] = new Text[0];
    return createIteratorStack(numRows, numDocsPerRow, columnFamilies, otherColumnFamilies, docs, nullText);
  }
  
  private synchronized static SortedKeyValueIterator<Key,Value> createIteratorStack(int numRows, int numDocsPerRow, Text[] columnFamilies,
      Text[] otherColumnFamilies, HashSet<Text> docs, Text[] negatedColumns) throws IOException {
    TreeMap<Key,Value> inMemoryMap = createSortedMap(numRows, numDocsPerRow, columnFamilies, otherColumnFamilies, docs, negatedColumns);
    return new SortedMapIterator(inMemoryMap);
  }
  
  private synchronized static void cleanup() throws IOException {
    docid = 0;
  }
  
  public void testNull() {}
  
  @Override
  public void setUp() {
    Logger.getRootLogger().setLevel(Level.ERROR);
  }
  
  private static final int NUM_ROWS = 10;
  private static final int NUM_DOCIDS = 1000;
  
  public void test1() throws IOException {
    columnFamilies = new Text[2];
    columnFamilies[0] = new Text("C");
    columnFamilies[1] = new Text("E");
    otherColumnFamilies = new Text[4];
    otherColumnFamilies[0] = new Text("A");
    otherColumnFamilies[1] = new Text("B");
    otherColumnFamilies[2] = new Text("D");
    otherColumnFamilies[3] = new Text("F");
    
    hitRatio = 0.5f;
    SortedKeyValueIterator<Key,Value> source = createIteratorStack(NUM_ROWS, NUM_DOCIDS, columnFamilies, otherColumnFamilies, docs);
    Map<String,String> options = new HashMap<String,String>();
    options.put(IntersectingIterator.columnFamiliesOptionName, IntersectingIterator.encodeColumns(columnFamilies));
    IntersectingIterator iter = new IntersectingIterator();
    iter.init(source, options, env);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);
    int hitCount = 0;
    while (iter.hasTop()) {
      hitCount++;
      Key k = iter.getTopKey();
      assertTrue(docs.contains(k.getColumnQualifier()));
      iter.next();
    }
    assertTrue(hitCount == docs.size());
    cleanup();
  }
  
  public void test2() throws IOException {
    columnFamilies = new Text[3];
    columnFamilies[0] = new Text("A");
    columnFamilies[1] = new Text("E");
    columnFamilies[2] = new Text("G");
    otherColumnFamilies = new Text[4];
    otherColumnFamilies[0] = new Text("B");
    otherColumnFamilies[1] = new Text("C");
    otherColumnFamilies[2] = new Text("D");
    otherColumnFamilies[3] = new Text("F");
    
    hitRatio = 0.5f;
    SortedKeyValueIterator<Key,Value> source = createIteratorStack(NUM_ROWS, NUM_DOCIDS, columnFamilies, otherColumnFamilies, docs);
    Map<String,String> options = new HashMap<String,String>();
    options.put(IntersectingIterator.columnFamiliesOptionName, IntersectingIterator.encodeColumns(columnFamilies));
    IntersectingIterator iter = new IntersectingIterator();
    iter.init(source, options, env);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);
    int hitCount = 0;
    while (iter.hasTop()) {
      hitCount++;
      Key k = iter.getTopKey();
      assertTrue(docs.contains(k.getColumnQualifier()));
      iter.next();
    }
    assertTrue(hitCount == docs.size());
    cleanup();
  }
  
  public void test3() throws IOException {
    columnFamilies = new Text[6];
    columnFamilies[0] = new Text("C");
    columnFamilies[1] = new Text("E");
    columnFamilies[2] = new Text("G");
    columnFamilies[3] = new Text("H");
    columnFamilies[4] = new Text("I");
    columnFamilies[5] = new Text("J");
    otherColumnFamilies = new Text[4];
    otherColumnFamilies[0] = new Text("A");
    otherColumnFamilies[1] = new Text("B");
    otherColumnFamilies[2] = new Text("D");
    otherColumnFamilies[3] = new Text("F");
    
    hitRatio = 0.5f;
    SortedKeyValueIterator<Key,Value> source = createIteratorStack(NUM_ROWS, NUM_DOCIDS, columnFamilies, otherColumnFamilies, docs);
    SortedKeyValueIterator<Key,Value> source2 = createIteratorStack(NUM_ROWS, NUM_DOCIDS, columnFamilies, otherColumnFamilies, docs);
    ArrayList<SortedKeyValueIterator<Key,Value>> sourceIters = new ArrayList<SortedKeyValueIterator<Key,Value>>();
    sourceIters.add(source);
    sourceIters.add(source2);
    MultiIterator mi = new MultiIterator(sourceIters, false);
    Map<String,String> options = new HashMap<String,String>();
    options.put(IntersectingIterator.columnFamiliesOptionName, IntersectingIterator.encodeColumns(columnFamilies));
    IntersectingIterator iter = new IntersectingIterator();
    iter.init(mi, options, env);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);
    int hitCount = 0;
    while (iter.hasTop()) {
      hitCount++;
      Key k = iter.getTopKey();
      assertTrue(docs.contains(k.getColumnQualifier()));
      iter.next();
    }
    assertTrue(hitCount == docs.size());
    cleanup();
  }
  
  public void test4() throws IOException {
    columnFamilies = new Text[3];
    notFlags = new boolean[3];
    columnFamilies[0] = new Text("A");
    notFlags[0] = true;
    columnFamilies[1] = new Text("E");
    notFlags[1] = false;
    columnFamilies[2] = new Text("G");
    notFlags[2] = true;
    negatedColumns = new Text[2];
    negatedColumns[0] = new Text("A");
    negatedColumns[1] = new Text("G");
    otherColumnFamilies = new Text[4];
    otherColumnFamilies[0] = new Text("B");
    otherColumnFamilies[1] = new Text("C");
    otherColumnFamilies[2] = new Text("D");
    otherColumnFamilies[3] = new Text("F");
    
    hitRatio = 0.5f;
    SortedKeyValueIterator<Key,Value> source = createIteratorStack(NUM_ROWS, NUM_DOCIDS, columnFamilies, otherColumnFamilies, docs, negatedColumns);
    Map<String,String> options = new HashMap<String,String>();
    options.put(IntersectingIterator.columnFamiliesOptionName, IntersectingIterator.encodeColumns(columnFamilies));
    options.put(IntersectingIterator.notFlagOptionName, IntersectingIterator.encodeBooleans(notFlags));
    IntersectingIterator iter = new IntersectingIterator();
    iter.init(source, options, env);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);
    int hitCount = 0;
    while (iter.hasTop()) {
      hitCount++;
      Key k = iter.getTopKey();
      assertTrue(docs.contains(k.getColumnQualifier()));
      iter.next();
    }
    assertTrue(hitCount == docs.size());
    cleanup();
  }
}
