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
package org.apache.accumulo.server.master.tableOps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.MetadataTable;
import org.apache.accumulo.core.util.RootTable;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.curator.CuratorReaderWriter.Mutator;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.curator.CuratorReaderWriter;
import org.apache.accumulo.server.master.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.master.Master;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.util.MapCounter;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException.NoNodeException;

class CompactionDriver extends MasterRepo {
  
  private static final long serialVersionUID = 1L;
  
  private long compactId;
  private String tableId;
  private byte[] startRow;
  private byte[] endRow;
  
  public CompactionDriver(long compactId, String tableId, byte[] startRow, byte[] endRow) {
    
    this.compactId = compactId;
    this.tableId = tableId;
    this.startRow = startRow;
    this.endRow = endRow;
  }
  
  @Override
  public long isReady(long tid, Master master) throws Exception {
    
    String zCancelID = Constants.ZROOT + "/" + HdfsZooInstance.getInstance().getInstanceID() + Constants.ZTABLES + "/" + tableId
        + Constants.ZTABLE_COMPACT_CANCEL_ID;
    
    CuratorReaderWriter zoo = CuratorReaderWriter.getInstance();
    
    if (Long.parseLong(new String(zoo.getData(zCancelID, null))) >= compactId) {
      // compaction was canceled
      throw new ThriftTableOperationException(tableId, null, TableOperation.COMPACT, TableOperationExceptionType.OTHER, "Compaction canceled");
    }
    
    MapCounter<TServerInstance> serversToFlush = new MapCounter<TServerInstance>();
    Connector conn = master.getConnector();
    Scanner scanner = new IsolatedScanner(conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY));
    
    Range range = new KeyExtent(new Text(tableId), null, startRow == null ? null : new Text(startRow)).toMetadataRange();
    
    if (tableId.equals(MetadataTable.ID))
      range = range.clip(new Range(RootTable.EXTENT.getMetadataEntry(), false, null, true));
    
    scanner.setRange(range);
    MetadataTable.COMPACT_COLUMN.fetch(scanner);
    MetadataTable.DIRECTORY_COLUMN.fetch(scanner);
    scanner.fetchColumnFamily(MetadataTable.CURRENT_LOCATION_COLUMN_FAMILY);
    
    long t1 = System.currentTimeMillis();
    RowIterator ri = new RowIterator(scanner);
    
    int tabletsToWaitFor = 0;
    int tabletCount = 0;
    
    while (ri.hasNext()) {
      Iterator<Entry<Key,Value>> row = ri.next();
      long tabletCompactID = -1;
      
      TServerInstance server = null;
      
      Entry<Key,Value> entry = null;
      while (row.hasNext()) {
        entry = row.next();
        Key key = entry.getKey();
        
        if (MetadataTable.COMPACT_COLUMN.equals(key.getColumnFamily(), key.getColumnQualifier()))
          tabletCompactID = Long.parseLong(entry.getValue().toString());
        
        if (MetadataTable.CURRENT_LOCATION_COLUMN_FAMILY.equals(key.getColumnFamily()))
          server = new TServerInstance(entry.getValue(), key.getColumnQualifier());
      }
      
      if (tabletCompactID < compactId) {
        tabletsToWaitFor++;
        if (server != null)
          serversToFlush.increment(server, 1);
      }
      
      tabletCount++;
      
      Text tabletEndRow = new KeyExtent(entry.getKey().getRow(), (Text) null).getEndRow();
      if (tabletEndRow == null || (endRow != null && tabletEndRow.compareTo(new Text(endRow)) >= 0))
        break;
    }
    
    long scanTime = System.currentTimeMillis() - t1;
    
    Instance instance = master.getInstance();
    Tables.clearCache(instance);
    if (tabletCount == 0 && !Tables.exists(instance, tableId))
      throw new ThriftTableOperationException(tableId, null, TableOperation.COMPACT, TableOperationExceptionType.NOTFOUND, null);
    
    if (serversToFlush.size() == 0 && Tables.getTableState(instance, tableId) == TableState.OFFLINE)
      throw new ThriftTableOperationException(tableId, null, TableOperation.COMPACT, TableOperationExceptionType.OFFLINE, null);
    
    if (tabletsToWaitFor == 0)
      return 0;
    
    for (TServerInstance tsi : serversToFlush.keySet()) {
      try {
        final TServerConnection server = master.getConnection(tsi);
        if (server != null)
          server.compact(master.getMasterLock(), tableId, startRow, endRow);
      } catch (TException ex) {
        Logger.getLogger(CompactionDriver.class).error(ex.toString());
      }
    }
    
    long sleepTime = 500;
    
    if (serversToFlush.size() > 0)
      sleepTime = Collections.max(serversToFlush.values()) * sleepTime; // make wait time depend on the server with the most to
                                                                        // compact
      
    sleepTime = Math.max(2 * scanTime, sleepTime);
    
    sleepTime = Math.min(sleepTime, 30000);
    
    return sleepTime;
  }
  
  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {
    CompactRange.removeIterators(tid, tableId);
    Utils.getReadLock(tableId, tid).unlock();
    return null;
  }
  
  @Override
  public void undo(long tid, Master environment) throws Exception {
    
  }
  
}

public class CompactRange extends MasterRepo {
  
  private static final long serialVersionUID = 1L;
  private String tableId;
  private byte[] startRow;
  private byte[] endRow;
  private byte[] iterators;
  
  public static class CompactionIterators implements Writable {
    byte[] startRow;
    byte[] endRow;
    List<IteratorSetting> iterators;
    
    public CompactionIterators(byte[] startRow, byte[] endRow, List<IteratorSetting> iterators) {
      this.startRow = startRow;
      this.endRow = endRow;
      this.iterators = iterators;
    }
    
    public CompactionIterators() {
      startRow = null;
      endRow = null;
      iterators = Collections.emptyList();
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeBoolean(startRow != null);
      if (startRow != null) {
        out.writeInt(startRow.length);
        out.write(startRow);
      }
      
      out.writeBoolean(endRow != null);
      if (endRow != null) {
        out.writeInt(endRow.length);
        out.write(endRow);
      }
      
      out.writeInt(iterators.size());
      for (IteratorSetting is : iterators) {
        is.write(out);
      }
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      if (in.readBoolean()) {
        startRow = new byte[in.readInt()];
        in.readFully(startRow);
      } else {
        startRow = null;
      }
      
      if (in.readBoolean()) {
        endRow = new byte[in.readInt()];
        in.readFully(endRow);
      } else {
        endRow = null;
      }
      
      int num = in.readInt();
      iterators = new ArrayList<IteratorSetting>(num);
      
      for (int i = 0; i < num; i++) {
        iterators.add(new IteratorSetting(in));
      }
    }
    
    public Text getEndRow() {
      if (endRow == null)
        return null;
      return new Text(endRow);
    }
    
    public Text getStartRow() {
      if (startRow == null)
        return null;
      return new Text(startRow);
    }
    
    public List<IteratorSetting> getIterators() {
      return iterators;
    }
  }
  
  public CompactRange(String tableId, byte[] startRow, byte[] endRow, List<IteratorSetting> iterators) throws ThriftTableOperationException {
    this.tableId = tableId;
    this.startRow = startRow.length == 0 ? null : startRow;
    this.endRow = endRow.length == 0 ? null : endRow;
    
    if (iterators.size() > 0) {
      this.iterators = WritableUtils.toByteArray(new CompactionIterators(this.startRow, this.endRow, iterators));
    } else {
      iterators = null;
    }
    
    if (this.startRow != null && this.endRow != null && new Text(startRow).compareTo(new Text(endRow)) >= 0)
      throw new ThriftTableOperationException(tableId, null, TableOperation.COMPACT, TableOperationExceptionType.BAD_RANGE,
          "start row must be less than end row");
  }
  
  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return Utils.reserveTable(tableId, tid, false, true, TableOperation.COMPACT);
  }
  
  @Override
  public Repo<Master> call(final long tid, Master environment) throws Exception {
    String zTablePath = Constants.ZROOT + "/" + HdfsZooInstance.getInstance().getInstanceID() + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_COMPACT_ID;
    
    CuratorReaderWriter zoo = CuratorReaderWriter.getInstance();
    byte[] cid;
    try {
      cid = zoo.mutate(zTablePath, null, false, new Mutator() {
        @Override
        public byte[] mutate(byte[] currentValue) throws Exception {
          String cvs = new String(currentValue);
          String[] tokens = cvs.split(",");
          long flushID = Long.parseLong(new String(tokens[0]));
          flushID++;
          
          String txidString = String.format("%016x", tid);
          
          for (int i = 1; i < tokens.length; i++) {
            if (tokens[i].startsWith(txidString))
              continue; // skip self
              
            throw new ThriftTableOperationException(tableId, null, TableOperation.COMPACT, TableOperationExceptionType.OTHER,
                "Another compaction with iterators is running");
          }
          
          StringBuilder encodedIterators = new StringBuilder();
          
          if (iterators != null) {
            Hex hex = new Hex();
            encodedIterators.append(",");
            encodedIterators.append(txidString);
            encodedIterators.append("=");
            encodedIterators.append(new String(hex.encode(iterators)));
          }
          
          return ("" + flushID + encodedIterators).getBytes();
        }
      });
      
      return new CompactionDriver(Long.parseLong(new String(cid).split(",")[0]), tableId, startRow, endRow);
    } catch (NoNodeException nne) {
      throw new ThriftTableOperationException(tableId, null, TableOperation.COMPACT, TableOperationExceptionType.NOTFOUND, null);
    }
    
  }
  
  static void removeIterators(final long txid, String tableId) throws Exception {
    String zTablePath = Constants.ZROOT + "/" + HdfsZooInstance.getInstance().getInstanceID() + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_COMPACT_ID;
    
    CuratorReaderWriter zoo = CuratorReaderWriter.getInstance();
    
    zoo.mutate(zTablePath, null, false, new Mutator() {
      @Override
      public byte[] mutate(byte[] currentValue) throws Exception {
        String cvs = new String(currentValue);
        String[] tokens = cvs.split(",");
        long flushID = Long.parseLong(new String(tokens[0]));
        
        String txidString = String.format("%016x", txid);
        
        StringBuilder encodedIterators = new StringBuilder();
        for (int i = 1; i < tokens.length; i++) {
          if (tokens[i].startsWith(txidString))
            continue;
          encodedIterators.append(",");
          encodedIterators.append(tokens[i]);
        }
        
        return ("" + flushID + encodedIterators).getBytes();
      }
    });
    
  }
  
  @Override
  public void undo(long tid, Master environment) throws Exception {
    try {
      removeIterators(tid, tableId);
    } finally {
      Utils.unreserveTable(tableId, tid, false);
    }
  }
  
}
