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
package org.apache.accumulo.server.monitor;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.AccessControlException;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.HdfsZooInstance;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.gc.thrift.GCMonitorService;
import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.core.master.thrift.Compacting;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.LoggingRunnable;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.ServerServices.Service;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.Accumulo;
import org.apache.accumulo.server.monitor.servlets.DefaultServlet;
import org.apache.accumulo.server.monitor.servlets.GcStatusServlet;
import org.apache.accumulo.server.monitor.servlets.LogServlet;
import org.apache.accumulo.server.monitor.servlets.LoggersServlet;
import org.apache.accumulo.server.monitor.servlets.MasterServlet;
import org.apache.accumulo.server.monitor.servlets.OperationServlet;
import org.apache.accumulo.server.monitor.servlets.ProblemServlet;
import org.apache.accumulo.server.monitor.servlets.TServersServlet;
import org.apache.accumulo.server.monitor.servlets.TablesServlet;
import org.apache.accumulo.server.monitor.servlets.XMLServlet;
import org.apache.accumulo.server.monitor.servlets.trace.ListType;
import org.apache.accumulo.server.monitor.servlets.trace.ShowTrace;
import org.apache.accumulo.server.monitor.servlets.trace.Summary;
import org.apache.accumulo.server.problems.ProblemReports;
import org.apache.accumulo.server.problems.ProblemType;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.accumulo.server.util.EmbeddedWebServer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * Serve master statistics with an embedded web server.
 */
public class Monitor {
  private static final Logger log = Logger.getLogger(Monitor.class);
  
  public static final int REFRESH_TIME = 5;
  private static long lastRecalc = 0L;
  private static double totalIngestRate = 0.0;
  private static double totalIngestByteRate = 0.0;
  private static double totalQueryRate = 0.0;
  private static double totalQueryByteRate = 0.0;
  private static long totalEntries = 0L;
  private static int totalTabletCount = 0;
  private static int onlineTabletCount = 0;
  private static long totalHoldTime = 0;
  private static long totalLookups = 0;
  
  private static class MaxList<T> extends LinkedList<Pair<Long,T>> {
    private static final long serialVersionUID = 1L;
    
    private long maxDelta;
    
    public MaxList(long maxDelta) {
      this.maxDelta = maxDelta;
    }
    
    @Override
    public boolean add(Pair<Long,T> obj) {
      boolean result = super.add(obj);
      
      if (obj.getFirst() - get(0).getFirst() > maxDelta)
        remove(0);
      
      return result;
    }
    
  }
  
  private static final int MAX_TIME_PERIOD = 60 * 60 * 1000;
  private static List<Pair<Long,Double>> loadOverTime = Collections.synchronizedList(new MaxList<Double>(MAX_TIME_PERIOD));
  private static List<Pair<Long,Double>> ingestRateOverTime = Collections.synchronizedList(new MaxList<Double>(MAX_TIME_PERIOD));
  private static List<Pair<Long,Double>> ingestByteRateOverTime = Collections.synchronizedList(new MaxList<Double>(MAX_TIME_PERIOD));
  private static List<Pair<Long,Integer>> recoveriesOverTime = Collections.synchronizedList(new MaxList<Integer>(MAX_TIME_PERIOD));
  private static List<Pair<Long,Integer>> minorCompactionsOverTime = Collections.synchronizedList(new MaxList<Integer>(MAX_TIME_PERIOD));
  private static List<Pair<Long,Integer>> majorCompactionsOverTime = Collections.synchronizedList(new MaxList<Integer>(MAX_TIME_PERIOD));
  private static List<Pair<Long,Double>> lookupsOverTime = Collections.synchronizedList(new MaxList<Double>(MAX_TIME_PERIOD));
  private static List<Pair<Long,Integer>> queryRateOverTime = Collections.synchronizedList(new MaxList<Integer>(MAX_TIME_PERIOD));
  private static List<Pair<Long,Double>> queryByteRateOverTime = Collections.synchronizedList(new MaxList<Double>(MAX_TIME_PERIOD));
  private static LookupRateTracker lookupRateTracker = new LookupRateTracker();
  
  private static volatile boolean fetching = false;
  private static MasterMonitorInfo mmi;
  private static Map<String,Map<ProblemType,Integer>> problemSummary = Collections.emptyMap();
  private static Exception problemException;
  private static GCStatus gcStatus;
  
  public static Map<String,Double> summarizeTableStats(MasterMonitorInfo mmi) {
    Map<String,Double> compactingByTable = new HashMap<String,Double>();
    if (mmi != null && mmi.tServerInfo != null) {
      for (TabletServerStatus status : mmi.tServerInfo) {
        if (status != null && status.tableMap != null) {
          for (String table : status.tableMap.keySet()) {
            Double holdTime = compactingByTable.get(table);
            compactingByTable.put(table, Math.max(holdTime == null ? 0. : holdTime.doubleValue(), status.holdTime));
          }
        }
      }
    }
    return compactingByTable;
  }
  
  public static void add(TableInfo total, TableInfo more) {
    if (total.minor == null)
      total.minor = new Compacting();
    if (total.major == null)
      total.major = new Compacting();
    if (more.minor != null) {
      total.minor.running += more.minor.running;
      total.minor.queued += more.minor.queued;
    }
    if (more.major != null) {
      total.major.running += more.major.running;
      total.major.queued += more.major.queued;
    }
    total.onlineTablets += more.onlineTablets;
    total.recs += more.recs;
    total.recsInMemory += more.recsInMemory;
    total.tablets += more.tablets;
    total.ingestRate += more.ingestRate;
    total.ingestByteRate += more.ingestByteRate;
    total.queryRate += more.queryRate;
    total.queryByteRate += more.queryByteRate;
  }
  
  public static TableInfo summarizeTableStats(TabletServerStatus status) {
    TableInfo summary = new TableInfo();
    summary.major = new Compacting();
    summary.minor = new Compacting();
    for (TableInfo rates : status.tableMap.values()) {
      add(summary, rates);
    }
    return summary;
  }
  
  private static class LookupRateTracker {
    
    Map<String,Pair<Long,Long>> prevSamples = new HashMap<String,Pair<Long,Long>>();
    Map<String,Pair<Long,Long>> samples = new HashMap<String,Pair<Long,Long>>();
    Set<String> serversUpdated = new HashSet<String>();
    
    void startingUpdates() {
      serversUpdated.clear();
    }
    
    void updateTabletServer(String name, long sampleTime, long lookups) {
      Pair<Long,Long> newSample = new Pair<Long,Long>(sampleTime, lookups);
      Pair<Long,Long> lastSample = samples.get(name);
      
      if (lastSample == null || !lastSample.equals(newSample)) {
        samples.put(name, newSample);
        if (lastSample != null) {
          prevSamples.put(name, lastSample);
        }
      }
      serversUpdated.add(name);
    }
    
    void finishedUpdating() {
      // remove any tablet servers not updated
      samples.keySet().retainAll(serversUpdated);
      prevSamples.keySet().retainAll(serversUpdated);
    }
    
    double calculateLookupRate() {
      double totalRate = 0;
      
      for (Entry<String,Pair<Long,Long>> entry : prevSamples.entrySet()) {
        Pair<Long,Long> prevSample = entry.getValue();
        Pair<Long,Long> sample = samples.get(entry.getKey());
        
        totalRate += (sample.getSecond() - prevSample.getSecond()) / ((sample.getFirst() - prevSample.getFirst()) / (double) 1000);
      }
      
      return totalRate;
    }
  }
  
  public static void fetchData() {
    double totalIngestRate = 0.;
    double totalIngestByteRate = 0.;
    double totalQueryRate = 0.;
    double totalQueryByteRate = 0.;
    long totalEntries = 0;
    int totalTabletCount = 0;
    int onlineTabletCount = 0;
    long totalHoldTime = 0;
    long totalLookups = 0;
    boolean retry = true;
    
    // only recalc every so often
    long currentTime = System.currentTimeMillis();
    if (currentTime - lastRecalc < REFRESH_TIME * 1000)
      return;
    
    synchronized (Monitor.class) {
      if (fetching)
        return;
      fetching = true;
    }
    
    try {
      while (retry) {
        MasterClientService.Iface client = null;
        try {
          client = MasterClient.getConnection(HdfsZooInstance.getInstance(), false);
          mmi = client.getMasterStats(null, SecurityConstants.systemCredentials);
          Monitor.gcStatus = fetchGcStatus();
          retry = false;
        } catch (Exception e) {
          mmi = null;
          log.info("Error fetching stats: " + e);
          UtilWaitThread.sleep(1000);
        } finally {
          if (client != null)
            MasterClient.close(client);
        }
      }
      
      int majorCompactions = 0;
      int minorCompactions = 0;
      
      lookupRateTracker.startingUpdates();
      
      for (TabletServerStatus server : mmi.tServerInfo) {
        TableInfo summary = Monitor.summarizeTableStats(server);
        totalIngestRate += summary.ingestRate;
        totalIngestByteRate += summary.ingestByteRate;
        totalQueryRate += summary.queryRate;
        totalQueryByteRate += summary.queryByteRate;
        totalEntries += summary.recs;
        totalHoldTime += server.holdTime;
        totalLookups += server.lookups;
        majorCompactions += summary.major.running;
        minorCompactions += summary.minor.running;
        lookupRateTracker.updateTabletServer(server.name, server.lastContact, server.lookups);
      }
      
      lookupRateTracker.finishedUpdating();
      
      for (TableInfo tInfo : mmi.tableMap.values()) {
        totalTabletCount += tInfo.tablets;
        onlineTabletCount += tInfo.onlineTablets;
      }
      Monitor.totalIngestRate = totalIngestRate;
      totalIngestByteRate = totalIngestByteRate / 1000000.0;
      Monitor.totalIngestByteRate = totalIngestByteRate;
      Monitor.totalQueryRate = totalQueryRate;
      totalQueryByteRate = totalQueryByteRate / 1000000.0;
      Monitor.totalQueryByteRate = totalQueryByteRate;
      Monitor.totalEntries = totalEntries;
      Monitor.totalTabletCount = totalTabletCount;
      Monitor.onlineTabletCount = onlineTabletCount;
      Monitor.totalHoldTime = totalHoldTime;
      Monitor.totalLookups = totalLookups;
      
      ingestRateOverTime.add(new Pair<Long,Double>(currentTime, totalIngestRate));
      ingestByteRateOverTime.add(new Pair<Long,Double>(currentTime, totalIngestByteRate));
      recoveriesOverTime.add(new Pair<Long,Integer>(currentTime, mmi.recovery.size()));
      
      double totalLoad = 0.;
      for (TabletServerStatus status : mmi.tServerInfo) {
        if (status != null)
          totalLoad += status.osLoad;
      }
      loadOverTime.add(new Pair<Long,Double>(currentTime, totalLoad));
      
      minorCompactionsOverTime.add(new Pair<Long,Integer>(currentTime, minorCompactions));
      majorCompactionsOverTime.add(new Pair<Long,Integer>(currentTime, majorCompactions));
      
      lookupsOverTime.add(new Pair<Long,Double>(currentTime, lookupRateTracker.calculateLookupRate()));
      
      queryRateOverTime.add(new Pair<Long,Integer>(currentTime, (int) totalQueryRate));
      queryByteRateOverTime.add(new Pair<Long,Double>(currentTime, totalQueryByteRate));
      
      try {
        Monitor.problemSummary = ProblemReports.getInstance().summarize();
        Monitor.problemException = null;
      } catch (Exception e) {
        log.info("Failed to obtain problem reports ", e);
        Monitor.problemSummary = Collections.emptyMap();
        Monitor.problemException = e;
      }
      
    } finally {
      synchronized (Monitor.class) {
        fetching = false;
        lastRecalc = currentTime;
      }
    }
  }
  
  private static GCStatus fetchGcStatus() {
    GCStatus result = null;
    try {
      // Read the gc location from its lock
      Instance instance = HdfsZooInstance.getInstance();
      String zooKeepers = instance.getZooKeepers();
      log.debug("connecting to zookeepers " + zooKeepers);
      ZooKeeper zk = new ZooKeeper(zooKeepers, (int) AccumuloConfiguration.getSystemConfiguration().getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT),
          new Watcher() {
            @Override
            public void process(WatchedEvent event) {}
          });
      try {
        String path = ZooUtil.getRoot(HdfsZooInstance.getInstance()) + Constants.ZGC_LOCK;
        List<String> locks = zk.getChildren(path, null);
        if (locks != null && locks.size() > 0) {
          Collections.sort(locks);
          InetSocketAddress address = new ServerServices(new String(zk.getData(path + "/" + locks.get(0), null, null))).getAddress(Service.GC_CLIENT);
          GCMonitorService.Iface client = ThriftUtil.getClient(new GCMonitorService.Client.Factory(), address, AccumuloConfiguration.getSystemConfiguration());
          try {
            result = client.getStatus(null, SecurityConstants.systemCredentials);
          } finally {
            ThriftUtil.returnClient(client);
          }
        }
      } finally {
        zk.close();
      }
    } catch (Exception ex) {
      log.warn("Unable to contact the garbage collector", ex);
    }
    return result;
  }
  
  public static void main(String[] args) {
    
    // Set security policy
    System.setProperty("java.security.policy", System.getenv("ACCUMULO_HOME") + "/conf/monitor.security.policy");
    
    // Set security manager (which uses permissions in monitor.security.policy) that
    // will log an error if any permission is denied
    System.setSecurityManager(new SecurityManager() {
      
      private final Logger log = Logger.getLogger(SecurityManager.class);
      
      @Override
      public void checkPermission(Permission p, Object context) {
        try {
          super.checkPermission(p, context);
        } catch (AccessControlException e) {
          log.error("Permission " + p + " denied in context " + context + ":", e);
          throw e;
        }
      }
      
      @Override
      public void checkPermission(Permission p) {
        try {
          super.checkPermission(p);
        } catch (AccessControlException e) {
          log.error("Permission " + p + " denied!", e);
          throw e;
        }
      }
    });
    new Monitor().run(args);
  }
  
  private static long START_TIME;
  
  public void run(String[] args) {
    Monitor.START_TIME = System.currentTimeMillis();
    try {
      Accumulo.init("monitor");
      Accumulo.enableTracing(Accumulo.getLocalAddress(args).toString(), "monitor");
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
    int port = AccumuloConfiguration.getSystemConfiguration().getPort(Property.MONITOR_PORT);
    EmbeddedWebServer server;
    try {
      log.debug("Creating monitor on port " + port);
      server = EmbeddedWebServer.create(port);
    } catch (Throwable ex) {
      log.error("Unable to start embedded web server", ex);
      throw new RuntimeException(ex);
    }
    
    server.addServlet(DefaultServlet.class, "/");
    server.addServlet(OperationServlet.class, "/op");
    server.addServlet(MasterServlet.class, "/master");
    server.addServlet(TablesServlet.class, "/tables");
    server.addServlet(TServersServlet.class, "/tservers");
    server.addServlet(LoggersServlet.class, "/loggers");
    server.addServlet(ProblemServlet.class, "/problems");
    server.addServlet(GcStatusServlet.class, "/gc");
    server.addServlet(LogServlet.class, "/log");
    server.addServlet(XMLServlet.class, "/xml");
    server.addServlet(Summary.class, "/trace/summary");
    server.addServlet(ListType.class, "/trace/listType");
    server.addServlet(ShowTrace.class, "/trace/show");
    LogService.startLogListener();
    server.start();
    
    new Daemon(new LoggingRunnable(log, new ZooKeeperStatus()), "ZooKeeperStatus").start();
    
    // need to regularly fetch data so plot data is updated
    new Daemon(new LoggingRunnable(log, new Runnable() {
      
      @Override
      public void run() {
        while (true) {
          try {
            Monitor.fetchData();
          } catch (Exception e) {
            log.warn(e.getMessage(), e);
          }
          
          UtilWaitThread.sleep(333);
        }
        
      }
    }), "Data fetcher").start();
  }
  
  public static MasterMonitorInfo getMmi() {
    return mmi;
  }
  
  public static int getTotalTabletCount() {
    return totalTabletCount;
  }
  
  public static int getOnlineTabletCount() {
    return onlineTabletCount;
  }
  
  public static long getTotalEntries() {
    return totalEntries;
  }
  
  public static double getTotalIngestRate() {
    return totalIngestRate;
  }
  
  public static double getTotalIngestByteRate() {
    return totalIngestByteRate;
  }
  
  public static double getTotalQueryRate() {
    return totalQueryRate;
  }
  
  public static double getTotalQueryByteRate() {
    return totalQueryByteRate;
  }
  
  public static long getTotalHoldTime() {
    return totalHoldTime;
  }
  
  public static Exception getProblemException() {
    return problemException;
  }
  
  public static Map<String,Map<ProblemType,Integer>> getProblemSummary() {
    return problemSummary;
  }
  
  public static GCStatus getGcStatus() {
    return gcStatus;
  }
  
  public static long getTotalLookups() {
    return totalLookups;
  }
  
  public static long getStartTime() {
    return START_TIME;
  }
  
  public static List<Pair<Long,Double>> getLoadOverTime() {
    synchronized (loadOverTime) {
      return new ArrayList<Pair<Long,Double>>(loadOverTime);
    }
  }
  
  public static List<Pair<Long,Double>> getIngestRateOverTime() {
    synchronized (ingestRateOverTime) {
      return new ArrayList<Pair<Long,Double>>(ingestRateOverTime);
    }
  }
  
  public static List<Pair<Long,Double>> getIngestByteRateOverTime() {
    synchronized (ingestByteRateOverTime) {
      return new ArrayList<Pair<Long,Double>>(ingestByteRateOverTime);
    }
  }
  
  public static List<Pair<Long,Integer>> getRecoveriesOverTime() {
    synchronized (recoveriesOverTime) {
      return new ArrayList<Pair<Long,Integer>>(recoveriesOverTime);
    }
  }
  
  public static List<Pair<Long,Integer>> getMinorCompactionsOverTime() {
    synchronized (minorCompactionsOverTime) {
      return new ArrayList<Pair<Long,Integer>>(minorCompactionsOverTime);
    }
  }
  
  public static List<Pair<Long,Integer>> getMajorCompactionsOverTime() {
    synchronized (majorCompactionsOverTime) {
      return new ArrayList<Pair<Long,Integer>>(majorCompactionsOverTime);
    }
  }
  
  public static List<Pair<Long,Double>> getLookupsOverTime() {
    synchronized (lookupsOverTime) {
      return new ArrayList<Pair<Long,Double>>(lookupsOverTime);
    }
  }
  
  public static List<Pair<Long,Integer>> getQueryRateOverTime() {
    synchronized (queryRateOverTime) {
      return new ArrayList<Pair<Long,Integer>>(queryRateOverTime);
    }
  }
  
  public static List<Pair<Long,Double>> getQueryByteRateOverTime() {
    synchronized (queryByteRateOverTime) {
      return new ArrayList<Pair<Long,Double>>(queryByteRateOverTime);
    }
  }
}
