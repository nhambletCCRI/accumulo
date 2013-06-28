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
package org.apache.accumulo.server.master;

import static java.lang.Math.min;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.RootTable;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.master.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.master.Master.TabletGoalState;
import org.apache.accumulo.server.master.state.Assignment;
import org.apache.accumulo.server.master.state.DistributedStoreException;
import org.apache.accumulo.server.master.state.MergeInfo;
import org.apache.accumulo.server.master.state.MergeState;
import org.apache.accumulo.server.master.state.MergeStats;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TableCounts;
import org.apache.accumulo.server.master.state.TableStats;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletState;
import org.apache.accumulo.server.master.state.TabletStateStore;
import org.apache.accumulo.server.master.state.tables.TableManager;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.accumulo.server.tabletserver.TabletTime;
import org.apache.accumulo.server.util.MetadataTable;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;

class TabletGroupWatcher extends Daemon {
  
  private final Master master;
  final TabletStateStore store;
  final TabletGroupWatcher dependentWatcher;
  
  final TableStats stats = new TableStats();
  
  TabletGroupWatcher(Master master, TabletStateStore store, TabletGroupWatcher dependentWatcher) {
    this.master = master;
    this.store = store;
    this.dependentWatcher = dependentWatcher;
  }
  
  Map<Text,TableCounts> getStats() {
    return stats.getLast();
  }
  
  TableCounts getStats(Text tableId) {
    return stats.getLast(tableId);
  }
  
  @Override
  public void run() {
    
    Thread.currentThread().setName("Watching " + store.name());
    int[] oldCounts = new int[TabletState.values().length];
    EventCoordinator.Listener eventListener = this.master.nextEvent.getListener();
    
    while (this.master.stillMaster()) {
      int totalUnloaded = 0;
      int unloaded = 0;
      try {
        Map<Text,MergeStats> mergeStatsCache = new HashMap<Text,MergeStats>();
        
        // Get the current status for the current list of tservers
        SortedMap<TServerInstance,TabletServerStatus> currentTServers = new TreeMap<TServerInstance,TabletServerStatus>();
        for (TServerInstance entry : this.master.tserverSet.getCurrentServers()) {
          currentTServers.put(entry, this.master.tserverStatus.get(entry));
        }
        
        if (currentTServers.size() == 0) {
          eventListener.waitForEvents(Master.TIME_TO_WAIT_BETWEEN_SCANS);
          continue;
        }
        
        // Don't move tablets to servers that are shutting down
        SortedMap<TServerInstance,TabletServerStatus> destinations = new TreeMap<TServerInstance,TabletServerStatus>(currentTServers);
        destinations.keySet().removeAll(this.master.serversToShutdown);
        
        List<Assignment> assignments = new ArrayList<Assignment>();
        List<Assignment> assigned = new ArrayList<Assignment>();
        List<TabletLocationState> assignedToDeadServers = new ArrayList<TabletLocationState>();
        Map<KeyExtent,TServerInstance> unassigned = new HashMap<KeyExtent,TServerInstance>();
        
        int[] counts = new int[TabletState.values().length];
        stats.begin();
        // Walk through the tablets in our store, and work tablets
        // towards their goal
        for (TabletLocationState tls : store) {
          if (tls == null) {
            continue;
          }
          // ignore entries for tables that do not exist in zookeeper
          if (TableManager.getInstance().getTableState(tls.extent.getTableId().toString()) == null)
            continue;
          
          // Don't overwhelm the tablet servers with work
          if (unassigned.size() + unloaded > Master.MAX_TSERVER_WORK_CHUNK * currentTServers.size()) {
            flushChanges(destinations, assignments, assigned, assignedToDeadServers, unassigned);
            assignments.clear();
            assigned.clear();
            assignedToDeadServers.clear();
            unassigned.clear();
            unloaded = 0;
            eventListener.waitForEvents(Master.TIME_TO_WAIT_BETWEEN_SCANS);
          }
          Text tableId = tls.extent.getTableId();
          MergeStats mergeStats = mergeStatsCache.get(tableId);
          if (mergeStats == null) {
            mergeStatsCache.put(tableId, mergeStats = new MergeStats(this.master.getMergeInfo(tls.extent)));
          }
          TabletGoalState goal = this.master.getGoalState(tls, mergeStats.getMergeInfo());
          TServerInstance server = tls.getServer();
          TabletState state = tls.getState(currentTServers.keySet());
          stats.update(tableId, state);
          mergeStats.update(tls.extent, state, tls.chopped, !tls.walogs.isEmpty());
          sendChopRequest(mergeStats.getMergeInfo(), state, tls);
          sendSplitRequest(mergeStats.getMergeInfo(), state, tls);
          
          // Always follow through with assignments
          if (state == TabletState.ASSIGNED) {
            goal = TabletGoalState.HOSTED;
          }
          
          // if we are shutting down all the tabletservers, we have to do it in order
          if (goal == TabletGoalState.UNASSIGNED && state == TabletState.HOSTED) {
            if (this.master.serversToShutdown.equals(currentTServers.keySet())) {
              if (dependentWatcher != null && dependentWatcher.assignedOrHosted() > 0) {
                goal = TabletGoalState.HOSTED;
              }
            }
          }
          
          if (goal == TabletGoalState.HOSTED) {
            if (state != TabletState.HOSTED && !tls.walogs.isEmpty()) {
              if (this.master.recoveryManager.recoverLogs(tls.extent, tls.walogs))
                continue;
            }
            switch (state) {
              case HOSTED:
                if (server.equals(this.master.migrations.get(tls.extent)))
                  this.master.migrations.remove(tls.extent);
                break;
              case ASSIGNED_TO_DEAD_SERVER:
                assignedToDeadServers.add(tls);
                if (server.equals(this.master.migrations.get(tls.extent)))
                  this.master.migrations.remove(tls.extent);
                // log.info("Current servers " + currentTServers.keySet());
                break;
              case UNASSIGNED:
                // maybe it's a finishing migration
                TServerInstance dest = this.master.migrations.get(tls.extent);
                if (dest != null) {
                  // if destination is still good, assign it
                  if (destinations.keySet().contains(dest)) {
                    assignments.add(new Assignment(tls.extent, dest));
                  } else {
                    // get rid of this migration
                    this.master.migrations.remove(tls.extent);
                    unassigned.put(tls.extent, server);
                  }
                } else {
                  unassigned.put(tls.extent, server);
                }
                break;
              case ASSIGNED:
                // Send another reminder
                assigned.add(new Assignment(tls.extent, tls.future));
                break;
            }
          } else {
            switch (state) {
              case UNASSIGNED:
                break;
              case ASSIGNED_TO_DEAD_SERVER:
                assignedToDeadServers.add(tls);
                // log.info("Current servers " + currentTServers.keySet());
                break;
              case HOSTED:
                TServerConnection conn = this.master.tserverSet.getConnection(server);
                if (conn != null) {
                  conn.unloadTablet(this.master.masterLock, tls.extent, goal != TabletGoalState.DELETED);
                  unloaded++;
                  totalUnloaded++;
                } else {
                  Master.log.warn("Could not connect to server " + server);
                }
                break;
              case ASSIGNED:
                break;
            }
          }
          counts[state.ordinal()]++;
        }
        
        flushChanges(destinations, assignments, assigned, assignedToDeadServers, unassigned);
        
        // provide stats after flushing changes to avoid race conditions w/ delete table
        stats.end();
        
        // Report changes
        for (TabletState state : TabletState.values()) {
          int i = state.ordinal();
          if (counts[i] > 0 && counts[i] != oldCounts[i]) {
            this.master.nextEvent.event("[%s]: %d tablets are %s", store.name(), counts[i], state.name());
          }
        }
        Master.log.debug(String.format("[%s]: scan time %.2f seconds", store.name(), stats.getScanTime() / 1000.));
        oldCounts = counts;
        if (totalUnloaded > 0) {
          this.master.nextEvent.event("[%s]: %d tablets unloaded", store.name(), totalUnloaded);
        }
        
        updateMergeState(mergeStatsCache);
        
        Master.log.debug(String.format("[%s] sleeping for %.2f seconds", store.name(), Master.TIME_TO_WAIT_BETWEEN_SCANS / 1000.));
        eventListener.waitForEvents(Master.TIME_TO_WAIT_BETWEEN_SCANS);
      } catch (Exception ex) {
        Master.log.error("Error processing table state for store " + store.name(), ex);
        UtilWaitThread.sleep(Master.WAIT_BETWEEN_ERRORS);
      }
    }
  }
  
  private int assignedOrHosted() {
    int result = 0;
    for (TableCounts counts : stats.getLast().values()) {
      result += counts.assigned() + counts.hosted();
    }
    return result;
  }
  
  private void sendSplitRequest(MergeInfo info, TabletState state, TabletLocationState tls) {
    // Already split?
    if (!info.getState().equals(MergeState.SPLITTING))
      return;
    // Merges don't split
    if (!info.isDelete())
      return;
    // Online and ready to split?
    if (!state.equals(TabletState.HOSTED))
      return;
    // Does this extent cover the end points of the delete?
    KeyExtent range = info.getExtent();
    if (tls.extent.overlaps(range)) {
      for (Text splitPoint : new Text[] {range.getPrevEndRow(), range.getEndRow()}) {
        if (splitPoint == null)
          continue;
        if (!tls.extent.contains(splitPoint))
          continue;
        if (splitPoint.equals(tls.extent.getEndRow()))
          continue;
        if (splitPoint.equals(tls.extent.getPrevEndRow()))
          continue;
        try {
          TServerConnection conn;
          conn = this.master.tserverSet.getConnection(tls.current);
          if (conn != null) {
            Master.log.info("Asking " + tls.current + " to split " + tls.extent + " at " + splitPoint);
            conn.splitTablet(this.master.masterLock, tls.extent, splitPoint);
          } else {
            Master.log.warn("Not connected to server " + tls.current);
          }
        } catch (NotServingTabletException e) {
          Master.log.debug("Error asking tablet server to split a tablet: " + e);
        } catch (Exception e) {
          Master.log.warn("Error asking tablet server to split a tablet: " + e);
        }
      }
    }
  }
  
  private void sendChopRequest(MergeInfo info, TabletState state, TabletLocationState tls) {
    // Don't bother if we're in the wrong state
    if (!info.getState().equals(MergeState.WAITING_FOR_CHOPPED))
      return;
    // Tablet must be online
    if (!state.equals(TabletState.HOSTED))
      return;
    // Tablet isn't already chopped
    if (tls.chopped)
      return;
    // Tablet ranges intersect
    if (info.needsToBeChopped(tls.extent)) {
      TServerConnection conn;
      try {
        conn = this.master.tserverSet.getConnection(tls.current);
        if (conn != null) {
          Master.log.info("Asking " + tls.current + " to chop " + tls.extent);
          conn.chop(this.master.masterLock, tls.extent);
        } else {
          Master.log.warn("Could not connect to server " + tls.current);
        }
      } catch (TException e) {
        Master.log.warn("Communications error asking tablet server to chop a tablet");
      }
    }
  }
  
  private void updateMergeState(Map<Text,MergeStats> mergeStatsCache) {
    for (MergeStats stats : mergeStatsCache.values()) {
      try {
        MergeState update = stats.nextMergeState(this.master.getConnector(), this.master);
        // when next state is MERGING, its important to persist this before
        // starting the merge... the verification check that is done before
        // moving into the merging state could fail if merge starts but does
        // not finish
        if (update == MergeState.COMPLETE)
          update = MergeState.NONE;
        if (update != stats.getMergeInfo().getState()) {
          this.master.setMergeState(stats.getMergeInfo(), update);
        }
        
        if (update == MergeState.MERGING) {
          try {
            if (stats.getMergeInfo().isDelete()) {
              deleteTablets(stats.getMergeInfo());
            } else {
              mergeMetadataRecords(stats.getMergeInfo());
            }
            this.master.setMergeState(stats.getMergeInfo(), update = MergeState.COMPLETE);
          } catch (Exception ex) {
            Master.log.error("Unable merge metadata table records", ex);
          }
        }
      } catch (Exception ex) {
        Master.log.error("Unable to update merge state for merge " + stats.getMergeInfo().getExtent(), ex);
      }
    }
  }
  
  private void deleteTablets(MergeInfo info) throws AccumuloException {
    KeyExtent extent = info.getExtent();
    String targetSystemTable = extent.isMeta() ? RootTable.NAME : MetadataTable.NAME;
    Master.log.debug("Deleting tablets for " + extent);
    char timeType = '\0';
    KeyExtent followingTablet = null;
    if (extent.getEndRow() != null) {
      Key nextExtent = new Key(extent.getEndRow()).followingKey(PartialKey.ROW);
      followingTablet = getHighTablet(new KeyExtent(extent.getTableId(), nextExtent.getRow(), extent.getEndRow()));
      Master.log.debug("Found following tablet " + followingTablet);
    }
    try {
      Connector conn = this.master.getConnector();
      Text start = extent.getPrevEndRow();
      if (start == null) {
        start = new Text();
      }
      Master.log.debug("Making file deletion entries for " + extent);
      Range deleteRange = new Range(KeyExtent.getMetadataEntry(extent.getTableId(), start), false, KeyExtent.getMetadataEntry(extent.getTableId(),
          extent.getEndRow()), true);
      Scanner scanner = conn.createScanner(targetSystemTable, Authorizations.EMPTY);
      scanner.setRange(deleteRange);
      MetadataTable.DIRECTORY_COLUMN.fetch(scanner);
      MetadataTable.TIME_COLUMN.fetch(scanner);
      scanner.fetchColumnFamily(MetadataTable.DATAFILE_COLUMN_FAMILY);
      scanner.fetchColumnFamily(MetadataTable.CURRENT_LOCATION_COLUMN_FAMILY);
      Set<FileRef> datafiles = new TreeSet<FileRef>();
      for (Entry<Key,Value> entry : scanner) {
        Key key = entry.getKey();
        if (key.compareColumnFamily(MetadataTable.DATAFILE_COLUMN_FAMILY) == 0) {
          datafiles.add(new FileRef(this.master.fs, key));
          if (datafiles.size() > 1000) {
            MetadataTable.addDeleteEntries(extent, datafiles, SecurityConstants.getSystemCredentials());
            datafiles.clear();
          }
        } else if (MetadataTable.TIME_COLUMN.hasColumns(key)) {
          timeType = entry.getValue().toString().charAt(0);
        } else if (key.compareColumnFamily(MetadataTable.CURRENT_LOCATION_COLUMN_FAMILY) == 0) {
          throw new IllegalStateException("Tablet " + key.getRow() + " is assigned during a merge!");
        } else if (MetadataTable.DIRECTORY_COLUMN.hasColumns(key)) {
          datafiles.add(new FileRef(this.master.fs, key));
          if (datafiles.size() > 1000) {
            MetadataTable.addDeleteEntries(extent, datafiles, SecurityConstants.getSystemCredentials());
            datafiles.clear();
          }
        }
      }
      MetadataTable.addDeleteEntries(extent, datafiles, SecurityConstants.getSystemCredentials());
      BatchWriter bw = conn.createBatchWriter(targetSystemTable, new BatchWriterConfig());
      try {
        deleteTablets(info, deleteRange, bw, conn);
      } finally {
        bw.close();
      }
      
      if (followingTablet != null) {
        Master.log.debug("Updating prevRow of " + followingTablet + " to " + extent.getPrevEndRow());
        bw = conn.createBatchWriter(targetSystemTable, new BatchWriterConfig());
        try {
          Mutation m = new Mutation(followingTablet.getMetadataEntry());
          MetadataTable.PREV_ROW_COLUMN.put(m, KeyExtent.encodePrevEndRow(extent.getPrevEndRow()));
          MetadataTable.CHOPPED_COLUMN.putDelete(m);
          bw.addMutation(m);
          bw.flush();
        } finally {
          bw.close();
        }
      } else {
        // Recreate the default tablet to hold the end of the table
        Master.log.debug("Recreating the last tablet to point to " + extent.getPrevEndRow());
        MetadataTable.addTablet(new KeyExtent(extent.getTableId(), null, extent.getPrevEndRow()), Constants.DEFAULT_TABLET_LOCATION,
            SecurityConstants.getSystemCredentials(), timeType, this.master.masterLock);
      }
    } catch (Exception ex) {
      throw new AccumuloException(ex);
    }
  }
  
  private void mergeMetadataRecords(MergeInfo info) throws AccumuloException {
    KeyExtent range = info.getExtent();
    Master.log.debug("Merging metadata for " + range);
    KeyExtent stop = getHighTablet(range);
    Master.log.debug("Highest tablet is " + stop);
    Value firstPrevRowValue = null;
    Text stopRow = stop.getMetadataEntry();
    Text start = range.getPrevEndRow();
    if (start == null) {
      start = new Text();
    }
    Range scanRange = new Range(KeyExtent.getMetadataEntry(range.getTableId(), start), false, stopRow, false);
    String targetSystemTable = MetadataTable.NAME;
    if (range.isMeta()) {
      targetSystemTable = RootTable.NAME;
    }
    
    BatchWriter bw = null;
    try {
      long fileCount = 0;
      Connector conn = this.master.getConnector();
      // Make file entries in highest tablet
      bw = conn.createBatchWriter(targetSystemTable, new BatchWriterConfig());
      Scanner scanner = conn.createScanner(targetSystemTable, Authorizations.EMPTY);
      scanner.setRange(scanRange);
      MetadataTable.PREV_ROW_COLUMN.fetch(scanner);
      MetadataTable.TIME_COLUMN.fetch(scanner);
      MetadataTable.DIRECTORY_COLUMN.fetch(scanner);
      scanner.fetchColumnFamily(MetadataTable.DATAFILE_COLUMN_FAMILY);
      Mutation m = new Mutation(stopRow);
      String maxLogicalTime = null;
      for (Entry<Key,Value> entry : scanner) {
        Key key = entry.getKey();
        Value value = entry.getValue();
        if (key.getColumnFamily().equals(MetadataTable.DATAFILE_COLUMN_FAMILY)) {
          m.put(key.getColumnFamily(), key.getColumnQualifier(), value);
          fileCount++;
        } else if (MetadataTable.PREV_ROW_COLUMN.hasColumns(key) && firstPrevRowValue == null) {
          Master.log.debug("prevRow entry for lowest tablet is " + value);
          firstPrevRowValue = new Value(value);
        } else if (MetadataTable.TIME_COLUMN.hasColumns(key)) {
          maxLogicalTime = TabletTime.maxMetadataTime(maxLogicalTime, value.toString());
        } else if (MetadataTable.DIRECTORY_COLUMN.hasColumns(key)) {
          bw.addMutation(MetadataTable.createDeleteMutation(range.getTableId().toString(), entry.getValue().toString()));
        }
      }
      
      // read the logical time from the last tablet in the merge range, it is not included in
      // the loop above
      scanner = conn.createScanner(targetSystemTable, Authorizations.EMPTY);
      scanner.setRange(new Range(stopRow));
      MetadataTable.TIME_COLUMN.fetch(scanner);
      for (Entry<Key,Value> entry : scanner) {
        if (MetadataTable.TIME_COLUMN.hasColumns(entry.getKey())) {
          maxLogicalTime = TabletTime.maxMetadataTime(maxLogicalTime, entry.getValue().toString());
        }
      }
      
      if (maxLogicalTime != null)
        MetadataTable.TIME_COLUMN.put(m, new Value(maxLogicalTime.getBytes()));
      
      if (!m.getUpdates().isEmpty()) {
        bw.addMutation(m);
      }
      
      bw.flush();
      
      Master.log.debug("Moved " + fileCount + " files to " + stop);
      
      if (firstPrevRowValue == null) {
        Master.log.debug("tablet already merged");
        return;
      }
      
      stop.setPrevEndRow(KeyExtent.decodePrevEndRow(firstPrevRowValue));
      Mutation updatePrevRow = stop.getPrevRowUpdateMutation();
      Master.log.debug("Setting the prevRow for last tablet: " + stop);
      bw.addMutation(updatePrevRow);
      bw.flush();
      
      deleteTablets(info, scanRange, bw, conn);
      
      // Clean-up the last chopped marker
      m = new Mutation(stopRow);
      MetadataTable.CHOPPED_COLUMN.putDelete(m);
      bw.addMutation(m);
      bw.flush();
      
    } catch (Exception ex) {
      throw new AccumuloException(ex);
    } finally {
      if (bw != null)
        try {
          bw.close();
        } catch (Exception ex) {
          throw new AccumuloException(ex);
        }
    }
  }
  
  private void deleteTablets(MergeInfo info, Range scanRange, BatchWriter bw, Connector conn) throws TableNotFoundException, MutationsRejectedException {
    Scanner scanner;
    Mutation m;
    // Delete everything in the other tablets
    // group all deletes into tablet into one mutation, this makes tablets
    // either disappear entirely or not all.. this is important for the case
    // where the process terminates in the loop below...
    scanner = conn.createScanner(info.getExtent().isMeta() ? RootTable.NAME : MetadataTable.NAME, Authorizations.EMPTY);
    Master.log.debug("Deleting range " + scanRange);
    scanner.setRange(scanRange);
    RowIterator rowIter = new RowIterator(scanner);
    while (rowIter.hasNext()) {
      Iterator<Entry<Key,Value>> row = rowIter.next();
      m = null;
      while (row.hasNext()) {
        Entry<Key,Value> entry = row.next();
        Key key = entry.getKey();
        
        if (m == null)
          m = new Mutation(key.getRow());
        
        m.putDelete(key.getColumnFamily(), key.getColumnQualifier());
        Master.log.debug("deleting entry " + key);
      }
      bw.addMutation(m);
    }
    
    bw.flush();
  }
  
  private KeyExtent getHighTablet(KeyExtent range) throws AccumuloException {
    try {
      Connector conn = this.master.getConnector();
      Scanner scanner = conn.createScanner(range.isMeta() ? RootTable.NAME : MetadataTable.NAME, Authorizations.EMPTY);
      MetadataTable.PREV_ROW_COLUMN.fetch(scanner);
      KeyExtent start = new KeyExtent(range.getTableId(), range.getEndRow(), null);
      scanner.setRange(new Range(start.getMetadataEntry(), null));
      Iterator<Entry<Key,Value>> iterator = scanner.iterator();
      if (!iterator.hasNext()) {
        throw new AccumuloException("No last tablet for a merge " + range);
      }
      Entry<Key,Value> entry = iterator.next();
      KeyExtent highTablet = new KeyExtent(entry.getKey().getRow(), KeyExtent.decodePrevEndRow(entry.getValue()));
      if (highTablet.getTableId() != range.getTableId()) {
        throw new AccumuloException("No last tablet for merge " + range + " " + highTablet);
      }
      return highTablet;
    } catch (Exception ex) {
      throw new AccumuloException("Unexpected failure finding the last tablet for a merge " + range, ex);
    }
  }
  
  private void flushChanges(SortedMap<TServerInstance,TabletServerStatus> currentTServers, List<Assignment> assignments, List<Assignment> assigned,
      List<TabletLocationState> assignedToDeadServers, Map<KeyExtent,TServerInstance> unassigned) throws DistributedStoreException, TException {
    if (!assignedToDeadServers.isEmpty()) {
      int maxServersToShow = min(assignedToDeadServers.size(), 100);
      Master.log.debug(assignedToDeadServers.size() + " assigned to dead servers: " + assignedToDeadServers.subList(0, maxServersToShow) + "...");
      store.unassign(assignedToDeadServers);
      this.master.nextEvent.event("Marked %d tablets as unassigned because they don't have current servers", assignedToDeadServers.size());
    }
    
    if (!currentTServers.isEmpty()) {
      Map<KeyExtent,TServerInstance> assignedOut = new HashMap<KeyExtent,TServerInstance>();
      this.master.tabletBalancer.getAssignments(Collections.unmodifiableSortedMap(currentTServers), Collections.unmodifiableMap(unassigned), assignedOut);
      for (Entry<KeyExtent,TServerInstance> assignment : assignedOut.entrySet()) {
        if (unassigned.containsKey(assignment.getKey())) {
          if (assignment.getValue() != null) {
            Master.log.debug(store.name() + " assigning tablet " + assignment);
            assignments.add(new Assignment(assignment.getKey(), assignment.getValue()));
          }
        } else {
          Master.log.warn(store.name() + " load balancer assigning tablet that was not nominated for assignment " + assignment.getKey());
        }
      }
      if (!unassigned.isEmpty() && assignedOut.isEmpty())
        Master.log.warn("Load balancer failed to assign any tablets");
    }
    
    if (assignments.size() > 0) {
      Master.log.info(String.format("Assigning %d tablets", assignments.size()));
      store.setFutureLocations(assignments);
    }
    assignments.addAll(assigned);
    for (Assignment a : assignments) {
      TServerConnection conn = this.master.tserverSet.getConnection(a.server);
      if (conn != null) {
        conn.assignTablet(this.master.masterLock, a.tablet);
      } else {
        Master.log.warn("Could not connect to server " + a.server);
      }
    }
  }
  
}