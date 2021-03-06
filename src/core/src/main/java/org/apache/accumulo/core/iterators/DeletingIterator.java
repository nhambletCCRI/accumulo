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
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

public class DeletingIterator extends WrappingIterator {
  private boolean propogateDeletes;
  private Key workKey = new Key();
  
  public DeletingIterator deepCopy(IteratorEnvironment env) {
    return new DeletingIterator(this, env);
  }
  
  public DeletingIterator(DeletingIterator other, IteratorEnvironment env) {
    setSource(other.getSource().deepCopy(env));
    propogateDeletes = other.propogateDeletes;
  }
  
  public DeletingIterator() {}
  
  public DeletingIterator(SortedKeyValueIterator<Key,Value> iterator, boolean propogateDeletes) throws IOException {
    this.setSource(iterator);
    this.propogateDeletes = propogateDeletes;
    
    findTop();
  }
  
  @Override
  public void next() throws IOException {
    if (getSource().getTopKey().isDeleted())
      skipRowColumn();
    else
      getSource().next();
    findTop();
  }
  
  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    // do not want to seek to the middle of a row
    Range seekRange = IteratorUtil.maximizeStartKeyTimeStamp(range);
    
    getSource().seek(seekRange, columnFamilies, inclusive);
    findTop();
    
    if (range.getStartKey() != null) {
      while (getSource().hasTop() && getSource().getTopKey().compareTo(range.getStartKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME) < 0) {
        next();
      }
      
      while (hasTop() && range.beforeStartKey(getTopKey())) {
        next();
      }
    }
  }
  
  private void findTop() throws IOException {
    if (!propogateDeletes) {
      while (getSource().hasTop() && getSource().getTopKey().isDeleted()) {
        skipRowColumn();
      }
    }
  }
  
  private void skipRowColumn() throws IOException {
    workKey.set(getSource().getTopKey());
    
    Key keyToSkip = workKey;
    getSource().next();
    
    while (getSource().hasTop() && getSource().getTopKey().equals(keyToSkip, PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
      getSource().next();
    }
  }
  
  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }
}
