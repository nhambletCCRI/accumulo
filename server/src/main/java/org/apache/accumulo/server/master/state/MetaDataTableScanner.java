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
package org.apache.accumulo.server.master.state;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.CredentialHelper;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.util.MetadataTable;
import org.apache.accumulo.server.master.state.TabletLocationState.BadLocationStateException;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class MetaDataTableScanner implements Iterator<TabletLocationState> {
  private static final Logger log = Logger.getLogger(MetaDataTableScanner.class);
  
  BatchScanner mdScanner;
  Iterator<Entry<Key,Value>> iter;
  
  public MetaDataTableScanner(Instance instance, TCredentials auths, Range range, CurrentState state) {
    this(instance, auths, range, state, MetadataTable.NAME);
  }
  
  MetaDataTableScanner(Instance instance, TCredentials auths, Range range, CurrentState state, String tableName) {
    // scan over metadata table, looking for tablets in the wrong state based on the live servers and online tables
    try {
      Connector connector = instance.getConnector(auths.getPrincipal(), CredentialHelper.extractToken(auths));
      mdScanner = connector.createBatchScanner(tableName, Authorizations.EMPTY, 8);
      configureScanner(mdScanner, state);
      mdScanner.setRanges(Collections.singletonList(range));
      iter = mdScanner.iterator();
    } catch (Exception ex) {
      log.error(ex, ex);
      if (mdScanner != null)
        mdScanner.close();
      throw new RuntimeException(ex);
    }
  }
  
  static public void configureScanner(ScannerBase scanner, CurrentState state) {
    MetadataTable.PREV_ROW_COLUMN.fetch(scanner);
    scanner.fetchColumnFamily(MetadataTable.CURRENT_LOCATION_COLUMN_FAMILY);
    scanner.fetchColumnFamily(MetadataTable.FUTURE_LOCATION_COLUMN_FAMILY);
    scanner.fetchColumnFamily(MetadataTable.LOG_COLUMN_FAMILY);
    scanner.fetchColumnFamily(MetadataTable.CHOPPED_COLUMN_FAMILY);
    scanner.addScanIterator(new IteratorSetting(1000, "wholeRows", WholeRowIterator.class));
    IteratorSetting tabletChange = new IteratorSetting(1001, "tabletChange", TabletStateChangeIterator.class);
    if (state != null) {
      TabletStateChangeIterator.setCurrentServers(tabletChange, state.onlineTabletServers());
      TabletStateChangeIterator.setOnlineTables(tabletChange, state.onlineTables());
      TabletStateChangeIterator.setMerges(tabletChange, state.merges());
    }
    scanner.addScanIterator(tabletChange);
  }
  
  public MetaDataTableScanner(Instance instance, TCredentials auths, Range range) {
    this(instance, auths, range, MetadataTable.NAME);
  }
  
  public MetaDataTableScanner(Instance instance, TCredentials auths, Range range, String tableName) {
    this(instance, auths, range, null, tableName);
  }
  
  public void close() {
    if (iter != null) {
      mdScanner.close();
      iter = null;
    }
  }
  
  @Override
  public void finalize() {
    close();
  }
  
  @Override
  public boolean hasNext() {
    if (iter == null)
      return false;
    boolean result = iter.hasNext();
    if (!result) {
      close();
    }
    return result;
  }
  
  @Override
  public TabletLocationState next() {
    try {
      TabletLocationState toRet = fetch();
      return toRet;
    } catch (RuntimeException ex) {
      // something is wrong with the records in the !METADATA table, just skip over it
      log.error(ex, ex);
      mdScanner.close();
      return null;
    }
  }
  
  public static TabletLocationState createTabletLocationState(Key k, Value v) throws IOException, BadLocationStateException {
    final SortedMap<Key,Value> decodedRow = WholeRowIterator.decodeRow(k, v);
    KeyExtent extent = null;
    TServerInstance future = null;
    TServerInstance current = null;
    TServerInstance last = null;
    List<Collection<String>> walogs = new ArrayList<Collection<String>>();
    boolean chopped = false;
    
    for (Entry<Key,Value> entry : decodedRow.entrySet()) {
      Key key = entry.getKey();
      Text row = key.getRow();
      Text cf = key.getColumnFamily();
      Text cq = key.getColumnQualifier();
      
      if (cf.compareTo(MetadataTable.FUTURE_LOCATION_COLUMN_FAMILY) == 0) {
        TServerInstance location = new TServerInstance(entry.getValue(), cq);
        if (future != null) {
          throw new BadLocationStateException("found two assignments for the same extent " + key.getRow() + ": " + future + " and " + location);
        }
        future = location;
      } else if (cf.compareTo(MetadataTable.CURRENT_LOCATION_COLUMN_FAMILY) == 0) {
        TServerInstance location = new TServerInstance(entry.getValue(), cq);
        if (current != null) {
          throw new BadLocationStateException("found two locations for the same extent " + key.getRow() + ": " + current + " and " + location);
        }
        current = location;
      } else if (cf.compareTo(MetadataTable.LOG_COLUMN_FAMILY) == 0) {
        String[] split = entry.getValue().toString().split("\\|")[0].split(";");
        walogs.add(Arrays.asList(split));
      } else if (cf.compareTo(MetadataTable.LAST_LOCATION_COLUMN_FAMILY) == 0) {
        TServerInstance location = new TServerInstance(entry.getValue(), cq);
        if (last != null) {
          throw new BadLocationStateException("found two last locations for the same extent " + key.getRow() + ": " + last + " and " + location);
        }
        last = new TServerInstance(entry.getValue(), cq);
      } else if (cf.compareTo(MetadataTable.CHOPPED_COLUMN_FAMILY) == 0) {
        chopped = true;
      } else if (MetadataTable.PREV_ROW_COLUMN.equals(cf, cq)) {
        extent = new KeyExtent(row, entry.getValue());
      }
    }
    if (extent == null) {
      log.warn("No prev-row for key extent: " + decodedRow);
      return null;
    }
    return new TabletLocationState(extent, future, current, last, walogs, chopped);
  }
  
  private TabletLocationState fetch() {
    try {
      Entry<Key,Value> e = iter.next();
      return createTabletLocationState(e.getKey(), e.getValue());
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    } catch (BadLocationStateException ex) {
      throw new RuntimeException(ex);
    }
  }
  
  @Override
  public void remove() {
    throw new RuntimeException("Unimplemented");
  }
}
