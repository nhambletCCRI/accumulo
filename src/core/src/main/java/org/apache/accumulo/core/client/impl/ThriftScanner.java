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
package org.apache.accumulo.core.client.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.TabletLocator.TabletLocation;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.thrift.InitialScan;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.data.thrift.ScanResult;
import org.apache.accumulo.core.data.thrift.TKeyValue;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchScanIDException;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TooManyFilesException;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;

import cloudtrace.instrument.Span;
import cloudtrace.instrument.Trace;

public class ThriftScanner {
  private static final byte[] EMPTY_BYTES = new byte[0];
  private static final Logger log = Logger.getLogger(ThriftScanner.class);
  
  public static Map<TabletType,Set<String>> serversWaitedForWrites = new EnumMap<TabletType,Set<String>>(TabletType.class);
  
  static {
    for (TabletType ttype : TabletType.values()) {
      serversWaitedForWrites.put(ttype, Collections.synchronizedSet(new HashSet<String>()));
    }
  }
  
  public static boolean getBatchFromServer(AuthInfo credentials, Text startRow, KeyExtent extent, String server, SortedMap<Key,Value> results,
      SortedSet<Column> fetchedColumns, boolean skipStartKey, int size, Authorizations authorizations, boolean retry, AccumuloConfiguration conf)
      throws AccumuloException, AccumuloSecurityException, NotServingTabletException {
    Key startKey;
    
    if (fetchedColumns.size() > 0) {
      byte[] cf = fetchedColumns.first().columnFamily;
      byte[] cq = fetchedColumns.first().columnQualifier;
      byte[] cv = fetchedColumns.first().columnVisibility;
      
      startKey = new Key(TextUtil.getBytes(startRow), cf, cq == null ? EMPTY_BYTES : cq, cv == null ? EMPTY_BYTES : cv, Long.MAX_VALUE);
      
    } else
      startKey = new Key(startRow);
    
    if (skipStartKey)
      startKey = startKey.followingKey(PartialKey.ROW);
    else
      startKey.setTimestamp(Long.MAX_VALUE);
    
    return getBatchFromServer(credentials, startKey, (Key) null, extent, server, results, fetchedColumns, size, authorizations, retry, conf);
  }
  
  static boolean getBatchFromServer(AuthInfo credentials, Key key, Key endKey, KeyExtent extent, String server, SortedMap<Key,Value> results,
      SortedSet<Column> fetchedColumns, int size, Authorizations authorizations, boolean retry, AccumuloConfiguration conf) throws AccumuloException,
      AccumuloSecurityException, NotServingTabletException {
    return getBatchFromServer(credentials, new Range(key, true, endKey, true), extent, server, results, fetchedColumns, size, authorizations, retry, conf);
  }
  
  static boolean getBatchFromServer(AuthInfo credentials, Range range, KeyExtent extent, String server, SortedMap<Key,Value> results,
      SortedSet<Column> fetchedColumns, int size, Authorizations authorizations, boolean retry, AccumuloConfiguration conf) throws AccumuloException,
      AccumuloSecurityException, NotServingTabletException {
    if (server == null)
      throw new AccumuloException(new IOException());
    
    try {
      TabletClientService.Iface client = ThriftUtil.getTServerClient(server, conf);
      try {
        List<IterInfo> emptyList = Collections.emptyList();
        Map<String,Map<String,String>> emptyMap = Collections.emptyMap();
        // not reading whole rows (or stopping on row boundries) so there is no need to enable isolation below
        ScanState scanState = new ScanState(credentials, extent.getTableId(), authorizations, range, fetchedColumns, size, emptyList, emptyMap, false);
        
        TabletType ttype = TabletType.type(extent);
        boolean waitForWrites = !serversWaitedForWrites.get(ttype).contains(server);
        InitialScan isr = client.startScan(null, scanState.credentials, extent.toThrift(), scanState.range.toThrift(),
            Translator.translate(scanState.columns, Translator.CT), scanState.size, scanState.serverSideIteratorList, scanState.serverSideIteratorOptions,
            scanState.authorizations.getAuthorizations(), waitForWrites, scanState.isolated);
        if (waitForWrites)
          serversWaitedForWrites.get(ttype).add(server);
        
        Key.decompress(isr.result.results);
        
        for (TKeyValue kv : isr.result.results)
          results.put(new Key(kv.key), new Value(kv.value));
        
        client.closeScan(null, isr.scanID);
        
        return isr.result.more;
      } finally {
        ThriftUtil.returnClient((TServiceClient) client);
      }
    } catch (TApplicationException tae) {
      throw new AccumuloServerException(server, tae);
    } catch (TooManyFilesException e) {
      log.debug("Tablet (" + extent + ") has too many files " + server + " : " + e);
    } catch (TException e) {
      log.debug("Error getting transport to " + server + " : " + e);
    } catch (ThriftSecurityException e) {
      log.warn("Security Violation in scan request to " + server + ": " + e);
      throw new AccumuloSecurityException(e.user, e.code, e);
    }
    
    throw new AccumuloException("getBatchFromServer: failed");
  }
  
  public static class ScanState {
    
    boolean isolated;
    Text tableName;
    Text startRow;
    boolean skipStartRow;
    
    Range range;
    
    int size;
    
    AuthInfo credentials;
    Authorizations authorizations;
    List<Column> columns;
    
    TabletLocation prevLoc;
    Long scanID;
    
    boolean finished = false;
    
    List<IterInfo> serverSideIteratorList;
    
    Map<String,Map<String,String>> serverSideIteratorOptions;
    
    public ScanState(AuthInfo credentials, Text tableName, Authorizations authorizations, Range range, SortedSet<Column> fetchedColumns, int size,
        List<IterInfo> serverSideIteratorList, Map<String,Map<String,String>> serverSideIteratorOptions, boolean isolated) {
      this.credentials = credentials;
      this.authorizations = authorizations;
      
      columns = new ArrayList<Column>(fetchedColumns.size());
      for (Column column : fetchedColumns) {
        columns.add(column);
      }
      
      this.tableName = tableName;
      this.range = range;
      
      Key startKey = range.getStartKey();
      if (startKey == null) {
        startKey = new Key();
      }
      this.startRow = startKey.getRow();
      
      this.skipStartRow = false;
      
      this.size = size;
      
      this.serverSideIteratorList = serverSideIteratorList;
      this.serverSideIteratorOptions = serverSideIteratorOptions;
      
      this.isolated = isolated;
      
    }
  }
  
  public static class ScanTimedOutException extends IOException {
    
    private static final long serialVersionUID = 1L;
    
  }
  
  public static List<KeyValue> scan(Instance instance, AuthInfo credentials, ScanState scanState, int timeOut, AccumuloConfiguration conf)
      throws ScanTimedOutException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
    TabletLocation loc = null;
    
    long startTime = System.currentTimeMillis();
    String lastError = null;
    String error = null;
    int tooManyFilesCount = 0;
    
    List<KeyValue> results = null;
    
    Span span = Trace.start("scan");
    try {
      while (results == null && !scanState.finished) {
        
        if ((System.currentTimeMillis() - startTime) / 1000.0 > timeOut)
          throw new ScanTimedOutException();
        
        while (loc == null) {
          long currentTime = System.currentTimeMillis();
          if ((currentTime - startTime) / 1000.0 > timeOut)
            throw new ScanTimedOutException();
          
          Span locateSpan = Trace.start("scan:locateTablet");
          try {
            loc = TabletLocator.getInstance(instance, credentials, scanState.tableName).locateTablet(scanState.startRow, scanState.skipStartRow, false);
            if (loc == null) {
              error = "Failed to locate tablet for table : " + scanState.tableName + " row : " + scanState.startRow;
              if (!error.equals(lastError))
                log.debug(error);
              else if (log.isTraceEnabled())
                log.trace(error);
              lastError = error;
              UtilWaitThread.sleep(100);
            } else {
              // when a tablet splits we do want to continue scanning the low child
              // of the split if we are already passed it
              Range dataRange = loc.tablet_extent.toDataRange();
              
              if (scanState.range.getStartKey() != null && dataRange.afterEndKey(scanState.range.getStartKey())) {
                // go to the next tablet
                scanState.startRow = loc.tablet_extent.getEndRow();
                scanState.skipStartRow = true;
                loc = null;
              } else if (scanState.range.getEndKey() != null && dataRange.beforeStartKey(scanState.range.getEndKey())) {
                // should not happen
                throw new RuntimeException("Unexpected tablet, extent : " + loc.tablet_extent + "  range : " + scanState.range + " startRow : "
                    + scanState.startRow);
              }
            }
          } catch (AccumuloServerException e) {
            log.debug("Scan failed, server side exception : " + e.getMessage());
            throw e;
          } catch (AccumuloException e) {
            error = "exception from tablet loc " + e.getMessage();
            if (!error.equals(lastError))
              log.debug(error);
            else if (log.isTraceEnabled())
              log.trace(error);
            
            lastError = error;
            UtilWaitThread.sleep(100);
          } finally {
            locateSpan.stop();
          }
        }
        
        Span scanLocation = Trace.start("scan:location");
        scanLocation.data("tserver", loc.tablet_location);
        try {
          results = scan(loc, scanState, conf);
        } catch (AccumuloSecurityException e) {
          log.debug("Scan failed, security exception : " + e.getMessage() + " " + loc);
          throw e;
        } catch (TApplicationException tae) {
          throw new AccumuloServerException(loc.tablet_location, tae);
        } catch (NotServingTabletException e) {
          error = "Scan failed, not serving tablet " + loc;
          if (!error.equals(lastError))
            log.debug(error);
          else if (log.isTraceEnabled())
            log.trace(error);
          lastError = error;
          
          TabletLocator.getInstance(instance, credentials, scanState.tableName).invalidateCache(loc.tablet_extent);
          loc = null;
          
          // no need to try the current scan id somewhere else
          scanState.scanID = null;
          
          if (scanState.isolated)
            throw new IsolationException();
          
          UtilWaitThread.sleep(100);
        } catch (TException e) {
          TabletLocator.getInstance(instance, credentials, scanState.tableName).invalidateCache(loc.tablet_location);
          error = "Scan failed, thrift error " + e.getClass().getName() + "  " + e.getMessage() + " " + loc;
          if (!error.equals(lastError))
            log.debug(error);
          else if (log.isTraceEnabled())
            log.trace(error);
          lastError = error;
          loc = null;
          
          // do not want to continue using the same scan id, if a timeout occurred could cause a batch to be skipped
          // because a thread on the server side may still be processing the timed out continue scan
          scanState.scanID = null;
          
          if (scanState.isolated)
            throw new IsolationException();
          
          UtilWaitThread.sleep(100);
        } catch (NoSuchScanIDException e) {
          error = "Scan failed, no such scan id " + scanState.scanID + " " + loc;
          if (!error.equals(lastError))
            log.debug(error);
          else if (log.isTraceEnabled())
            log.trace(error);
          lastError = error;
          
          if (scanState.isolated)
            throw new IsolationException();
          
          scanState.scanID = null;
        } catch (TooManyFilesException e) {
          error = "Tablet has too many files " + loc + " retrying...";
          if (!error.equals(lastError)) {
            log.debug(error);
            tooManyFilesCount = 0;
          } else {
            tooManyFilesCount++;
            if (tooManyFilesCount == 300)
              log.warn(error);
            else if (log.isTraceEnabled())
              log.trace(error);
          }
          lastError = error;
          
          // not sure what state the scan session on the server side is
          // in after this occurs, so lets be cautious and start a new
          // scan session
          scanState.scanID = null;
          
          if (scanState.isolated)
            throw new IsolationException();
          
          UtilWaitThread.sleep(100);
        } finally {
          scanLocation.stop();
        }
      }
      
      if (results != null && results.size() == 0 && scanState.finished) {
        results = null;
      }
      
      return results;
    } finally {
      span.stop();
    }
  }
  
  private static List<KeyValue> scan(TabletLocation loc, ScanState scanState, AccumuloConfiguration conf) throws AccumuloSecurityException,
      NotServingTabletException, TException, NoSuchScanIDException, TooManyFilesException {
    if (scanState.finished)
      return null;
    
    OpTimer opTimer = new OpTimer(log, Level.TRACE);
    
    TabletClientService.Iface client = ThriftUtil.getTServerClient(loc.tablet_location, conf);
    
    String old = Thread.currentThread().getName();
    try {
      ScanResult sr;
      
      if (scanState.prevLoc != null && !scanState.prevLoc.equals(loc))
        scanState.scanID = null;
      
      scanState.prevLoc = loc;
      
      if (scanState.scanID == null) {
        String msg = "Starting scan tserver=" + loc.tablet_location + " tablet=" + loc.tablet_extent + " range=" + scanState.range + " ssil="
            + scanState.serverSideIteratorList + " ssio=" + scanState.serverSideIteratorOptions;
        Thread.currentThread().setName(msg);
        opTimer.start(msg);
        
        TabletType ttype = TabletType.type(loc.tablet_extent);
        boolean waitForWrites = !serversWaitedForWrites.get(ttype).contains(loc.tablet_location);
        InitialScan is = client.startScan(null, scanState.credentials, loc.tablet_extent.toThrift(), scanState.range.toThrift(),
            Translator.translate(scanState.columns, Translator.CT), scanState.size, scanState.serverSideIteratorList, scanState.serverSideIteratorOptions,
            scanState.authorizations.getAuthorizations(), waitForWrites, scanState.isolated);
        if (waitForWrites)
          serversWaitedForWrites.get(ttype).add(loc.tablet_location);
        
        sr = is.result;
        
        if (sr.more)
          scanState.scanID = is.scanID;
        else
          client.closeScan(null, is.scanID);
        
      } else {
        // log.debug("Calling continue scan : "+scanState.range+"  loc = "+loc);
        String msg = "Continuing scan tserver=" + loc.tablet_location + " scanid=" + scanState.scanID;
        Thread.currentThread().setName(msg);
        opTimer.start(msg);
        
        sr = client.continueScan(null, scanState.scanID);
        if (!sr.more) {
          client.closeScan(null, scanState.scanID);
          scanState.scanID = null;
        }
      }
      
      if (!sr.more) {
        // log.debug("No more : tab end row = "+loc.tablet_extent.getEndRow()+" range = "+scanState.range);
        if (loc.tablet_extent.getEndRow() == null) {
          scanState.finished = true;
          opTimer.stop("Completely finished scan in %DURATION% #results=" + sr.results.size());
        } else if (scanState.range.getEndKey() == null || !scanState.range.afterEndKey(new Key(loc.tablet_extent.getEndRow()).followingKey(PartialKey.ROW))) {
          scanState.startRow = loc.tablet_extent.getEndRow();
          scanState.skipStartRow = true;
          opTimer.stop("Finished scanning tablet in %DURATION% #results=" + sr.results.size());
        } else {
          scanState.finished = true;
          opTimer.stop("Completely finished scan in %DURATION% #results=" + sr.results.size());
        }
      } else {
        opTimer.stop("Finished scan in %DURATION% #results=" + sr.results.size() + " scanid=" + scanState.scanID);
      }
      
      Key.decompress(sr.results);
      
      if (sr.results.size() > 0 && !scanState.finished)
        scanState.range = new Range(new Key(sr.results.get(sr.results.size() - 1).key), false, scanState.range.getEndKey(), scanState.range.isEndKeyInclusive());
      
      List<KeyValue> results = new ArrayList<KeyValue>(sr.results.size());
      for (TKeyValue tkv : sr.results)
        results.add(new KeyValue(new Key(tkv.key), tkv.value));
      
      return results;
      
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } finally {
      ThriftUtil.returnClient((TServiceClient) client);
      Thread.currentThread().setName(old);
    }
  }
}
