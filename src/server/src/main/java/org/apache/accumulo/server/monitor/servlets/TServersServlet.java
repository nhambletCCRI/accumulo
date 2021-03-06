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
package org.apache.accumulo.server.monitor.servlets;

import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.tabletserver.thrift.ActionStats;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.server.master.state.TabletServerState;
import org.apache.accumulo.server.monitor.Monitor;
import org.apache.accumulo.server.monitor.util.Table;
import org.apache.accumulo.server.monitor.util.TableRow;
import org.apache.accumulo.server.monitor.util.celltypes.CompactionsType;
import org.apache.accumulo.server.monitor.util.celltypes.DurationType;
import org.apache.accumulo.server.monitor.util.celltypes.NumberType;
import org.apache.accumulo.server.monitor.util.celltypes.ProgressChartType;
import org.apache.accumulo.server.monitor.util.celltypes.TServerLinkType;
import org.apache.accumulo.server.monitor.util.celltypes.TableLinkType;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.accumulo.server.tabletserver.TabletStatsKeeper;
import org.apache.commons.codec.binary.Base64;

public class TServersServlet extends BasicServlet {
  
  private static final long serialVersionUID = 1L;
  private static final TabletServerStatus NO_STATUS = new TabletServerStatus();
  
  @Override
  protected String getTitle(HttpServletRequest req) {
    return "Tablet Server Status";
  }
  
  @Override
  protected void pageBody(HttpServletRequest req, HttpServletResponse response, StringBuilder sb) throws Exception {
    String tserverAddress = req.getParameter("s");
    log.debug("tserverAddr = " + tserverAddress);
    
    // Check to make sure tserver is a known address
    boolean tserverExists = false;
    if (tserverAddress != null && tserverAddress.isEmpty() == false) {
      for (TabletServerStatus ts : Monitor.getMmi().getTServerInfo()) {
        if (tserverAddress.equals(ts.getName())) {
          tserverExists = true;
          break;
        }
      }
    }
    
    if (tserverAddress == null || tserverAddress.isEmpty() || tserverExists == false) {
      doBadTserverList(req, sb);
      
      ArrayList<TabletServerStatus> tservers = new ArrayList<TabletServerStatus>();
      if (Monitor.getMmi() != null)
        tservers.addAll(Monitor.getMmi().tServerInfo);
      
      Table tServerList = new Table("tservers", "Tablet&nbsp;Servers");
      tServerList.setSubCaption("Click on the <span style='color: #0000ff;'>server address</span> to view detailed performance statistics for that server.");
      
      doTserverList(req, sb, tservers, tServerList);
      return;
    }
    
    double totalElapsedForAll = 0;
    long minorStdDev = 0;
    long splitStdDev = 0;
    long minorQueueStdDev = 0;
    long majorStdDev = 0;
    long majorQueueStdDev = 0;
    double currentMajorAvg = 0;
    long currentMinorAvg = 0;
    long currentMajorStdDev = 0;
    long currentMinorStdDev = 0;
    TabletStats total = new TabletStats(null, new ActionStats(), new ActionStats(), new ActionStats(), 0, 0, 0, 0);
    
    InetSocketAddress address = AddressUtil.parseAddress(tserverAddress, -1);
    TabletStats historical = new TabletStats(null, new ActionStats(), new ActionStats(), new ActionStats(), 0, 0, 0, 0);
    List<TabletStats> tsStats = new ArrayList<TabletStats>();
    try {
      TabletClientService.Iface client = ThriftUtil
          .getClient(new TabletClientService.Client.Factory(), address, AccumuloConfiguration.getSystemConfiguration());
      try {
        for (String tableId : Monitor.getMmi().tableMap.keySet()) {
          tsStats.addAll(client.getTabletStats(null, SecurityConstants.systemCredentials, tableId));
        }
        historical = client.getHistoricalStats(null, SecurityConstants.systemCredentials);
      } finally {
        ThriftUtil.returnClient(client);
      }
    } catch (Exception e) {
      banner(sb, "error", "No Such Tablet ServerAvailable");
      log.error(e, e);
      return;
    }
    
    Table perTabletResults = new Table("perTabletResults", "Detailed&nbsp;Current&nbsp;Operations");
    perTabletResults.setSubCaption("Per-tablet&nbsp;Details");
    perTabletResults.addSortableColumn("Table", new TableLinkType(), null);
    perTabletResults.addSortableColumn("Tablet");
    perTabletResults.addSortableColumn("Entries", new NumberType<Long>(), null);
    perTabletResults.addSortableColumn("Ingest", new NumberType<Long>(), null);
    perTabletResults.addSortableColumn("Query", new NumberType<Long>(), null);
    perTabletResults.addSortableColumn("Minor&nbsp;Avg", new NumberType<Double>(), null);
    perTabletResults.addSortableColumn("Minor&nbsp;Std&nbsp;Dev", new NumberType<Double>(), null);
    perTabletResults.addSortableColumn("Minor&nbsp;Avg&nbsp;e/s", new NumberType<Double>(), null);
    perTabletResults.addSortableColumn("Major&nbsp;Avg", new NumberType<Double>(), null);
    perTabletResults.addSortableColumn("Major&nbsp;Std&nbsp;Dev", new NumberType<Double>(), null);
    perTabletResults.addSortableColumn("Major&nbsp;Avg&nbsp;e/s", new NumberType<Double>(), null);
    
    for (TabletStats info : tsStats) {
      if (info.extent == null) {
        historical = info;
        continue;
      }
      total.numEntries += info.numEntries;
      TabletStatsKeeper.update(total.minor, info.minor);
      TabletStatsKeeper.update(total.major, info.major);
      
      KeyExtent extent = new KeyExtent(info.extent);
      String tableId = extent.getTableId().toString();
      MessageDigest digester = MessageDigest.getInstance("MD5");
      if (extent.getEndRow() != null && extent.getEndRow().getLength() > 0) {
        digester.update(extent.getEndRow().getBytes(), 0, extent.getEndRow().getLength());
      }
      String obscuredExtent = new String(Base64.encodeBase64(digester.digest()));
      String displayExtent = String.format("<code>[%s]</code>", obscuredExtent);
      
      TableRow row = perTabletResults.prepareRow();
      row.add(tableId);
      row.add(displayExtent);
      row.add(info.numEntries);
      row.add(info.ingestRate);
      row.add(info.queryRate);
      row.add(info.minor.num != 0 ? info.minor.elapsed / info.minor.num : null);
      row.add(stddev(info.minor.elapsed, info.minor.num, info.minor.sumDev));
      row.add(info.minor.elapsed != 0 ? info.minor.count / info.minor.elapsed : null);
      row.add(info.major.num != 0 ? info.major.elapsed / info.major.num : null);
      row.add(stddev(info.major.elapsed, info.major.num, info.major.sumDev));
      row.add(info.major.elapsed != 0 ? info.major.count / info.major.elapsed : null);
      perTabletResults.addRow(row);
    }
    
    // Calculate current averages oldServer adding in historical data
    if (total.minor.num != 0)
      currentMinorAvg = (long) (total.minor.elapsed / total.minor.num);
    if (total.minor.elapsed != 0 && total.minor.num != 0)
      currentMinorStdDev = (long) stddev(total.minor.elapsed, total.minor.num, total.minor.sumDev);
    if (total.major.num != 0)
      currentMajorAvg = total.major.elapsed / total.major.num;
    if (total.major.elapsed != 0 && total.major.num != 0 && total.major.elapsed > total.major.num)
      currentMajorStdDev = (long) stddev(total.major.elapsed, total.major.num, total.major.sumDev);
    
    // After these += operations, these variables are now total for current
    // tablets and historical tablets
    TabletStatsKeeper.update(total.minor, historical.minor);
    TabletStatsKeeper.update(total.major, historical.major);
    totalElapsedForAll += total.major.elapsed + historical.split.elapsed + total.minor.elapsed;
    
    minorStdDev = (long) stddev(total.minor.elapsed, total.minor.num, total.minor.sumDev);
    minorQueueStdDev = (long) stddev(total.minor.queueTime, total.minor.num, total.minor.queueSumDev);
    majorStdDev = (long) stddev(total.major.elapsed, total.major.num, total.major.sumDev);
    majorQueueStdDev = (long) stddev(total.major.queueTime, total.major.num, total.major.queueSumDev);
    splitStdDev = (long) stddev(historical.split.num, historical.split.elapsed, historical.split.sumDev);
    
    doDetailTable(req, sb, address, tsStats.size(), total, historical);
    doAllTimeTable(req, sb, total, historical, majorQueueStdDev, minorQueueStdDev, totalElapsedForAll, splitStdDev, majorStdDev, minorStdDev);
    doCurrentTabletOps(req, sb, currentMinorAvg, currentMinorStdDev, currentMajorAvg, currentMajorStdDev);
    perTabletResults.generate(req, sb);
  }
  
  private void doCurrentTabletOps(HttpServletRequest req, StringBuilder sb, double currentMinorAvg, double currentMinorStdDev, double currentMajorAvg,
      double currentMajorStdDev) {
    Table currentTabletOps = new Table("currentTabletOps", "Current&nbsp;Tablet&nbsp;Operation&nbsp;Results");
    currentTabletOps.addSortableColumn("Minor&nbsp;Average", new NumberType<Double>(), null);
    currentTabletOps.addSortableColumn("Minor&nbsp;Std&nbsp;Dev", new NumberType<Double>(), null);
    currentTabletOps.addSortableColumn("Major&nbsp;Avg", new NumberType<Double>(), null);
    currentTabletOps.addSortableColumn("Major&nbsp;Std&nbsp;Dev", new NumberType<Double>(), null);
    currentTabletOps.addRow(currentMinorAvg, currentMinorStdDev, currentMajorAvg, currentMajorStdDev);
    currentTabletOps.generate(req, sb);
  }
  
  private void doAllTimeTable(HttpServletRequest req, StringBuilder sb, TabletStats total, TabletStats historical, double majorQueueStdDev,
      double minorQueueStdDev, double totalElapsedForAll, double splitStdDev, double majorStdDev, double minorStdDev) {
    
    Table opHistoryDetails = new Table("opHistoryDetails", "All-Time&nbsp;Tablet&nbsp;Operation&nbsp;Results");
    opHistoryDetails.addSortableColumn("Operation");
    opHistoryDetails.addSortableColumn("Success", new NumberType<Integer>(), null);
    opHistoryDetails.addSortableColumn("Failure", new NumberType<Integer>(), null);
    opHistoryDetails.addSortableColumn("Average<br />Queue&nbsp;Time", new DurationType(), null);
    opHistoryDetails.addSortableColumn("Std.&nbsp;Dev.<br />Queue&nbsp;Time", new NumberType<Double>(), null);
    opHistoryDetails.addSortableColumn("Average<br />Time", new DurationType(), null);
    opHistoryDetails.addSortableColumn("Std.&nbsp;Dev.<br />Time", new NumberType<Double>(), null);
    opHistoryDetails.addSortableColumn("Percentage&nbsp;Time&nbsp;Spent", new ProgressChartType(totalElapsedForAll), null);
    
    opHistoryDetails.addRow("Split", historical.split.num, historical.split.fail, null, null,
        historical.split.num != 0 ? (long) (historical.split.elapsed / historical.split.num) : null, splitStdDev, historical.split.elapsed);
    opHistoryDetails.addRow("Major&nbsp;Compaction", total.major.num, total.major.fail, total.major.num != 0 ? (long) (total.major.queueTime / total.major.num)
        : null, majorQueueStdDev, total.major.num != 0 ? (long) (total.major.elapsed / total.major.num) : null, majorStdDev, total.major.elapsed);
    opHistoryDetails.addRow("Minor&nbsp;Compaction", total.minor.num, total.minor.fail, total.minor.num != 0 ? (long) (total.minor.queueTime / total.minor.num)
        : null, minorQueueStdDev, total.minor.num != 0 ? (long) (total.minor.elapsed / total.minor.num) : null, minorStdDev, total.minor.elapsed);
    opHistoryDetails.generate(req, sb);
  }
  
  private void doDetailTable(HttpServletRequest req, StringBuilder sb, InetSocketAddress address, int numTablets, TabletStats total, TabletStats historical) {
    Table detailTable = new Table("tServerDetail", "Details");
    detailTable.setSubCaption(address.getHostName() + ":" + address.getPort());
    detailTable.addSortableColumn("Hosted&nbsp;Tablets", new NumberType<Integer>(), null);
    detailTable.addSortableColumn("Entries", new NumberType<Long>(), null);
    detailTable.addSortableColumn("Minor&nbsp;Compacting", new NumberType<Integer>(), null);
    detailTable.addSortableColumn("Major&nbsp;Compacting", new NumberType<Integer>(), null);
    detailTable.addSortableColumn("Splitting", new NumberType<Integer>(), null);
    detailTable.addRow(numTablets, total.numEntries, total.minor.status, total.major.status, historical.split.status);
    detailTable.generate(req, sb);
  }
  
  private static double stddev(double elapsed, double num, double sumDev) {
    if (elapsed != 0 && num != 0 && elapsed > num)
      return Math.sqrt((1 / (elapsed / num)) * (sumDev - (num * Math.pow((elapsed / num), 2))));
    return 0;
  }
  
  private void doBadTserverList(HttpServletRequest req, StringBuilder sb) {
    if (Monitor.getMmi() != null && !Monitor.getMmi().badTServers.isEmpty()) {
      Table badTServerList = new Table("badtservers", "Non-Functioning&nbsp;Tablet&nbsp;Servers", "error");
      badTServerList.setSubCaption("The following tablet servers reported a status other than Online.");
      badTServerList.addSortableColumn("Tablet&nbsp;Server");
      badTServerList.addSortableColumn("Tablet&nbsp;Server&nbsp;Status");
      for (Entry<String,Byte> badserver : Monitor.getMmi().badTServers.entrySet())
        badTServerList.addRow(badserver.getKey(), TabletServerState.getStateById(badserver.getValue()).name());
      badTServerList.generate(req, sb);
    }
  }
  
  static void doTserverList(HttpServletRequest req, StringBuilder sb, List<TabletServerStatus> tservers, Table tServerList) {
    int guessHighLoad = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
    long now = System.currentTimeMillis();
    
    double avgLastContact = 0.;
    for (TabletServerStatus status : tservers) {
      avgLastContact += (now - status.lastContact);
    }
    
    double avgHostedTablets = 1.0;
    if (tservers.size() > 0) {
      avgLastContact /= tservers.size();
      avgHostedTablets = Monitor.getTotalTabletCount() / tservers.size();
    }
    
    tServerList.addSortableColumn("Server", new TServerLinkType(), null);
    tServerList.addSortableColumn("Hosted&nbsp;Tablets", new NumberType<Integer>((int) avgHostedTablets / 4, Math.max(1, (int) avgHostedTablets * 2)), null);
    tServerList.addSortableColumn("Last&nbsp;Contact", new DurationType(0l, (long) avgLastContact * 4), null);
    tServerList.addSortableColumn("Entries", new NumberType<Long>(), "The number of key/value pairs.");
    tServerList.addSortableColumn("Ingest", new NumberType<Long>(), "The number of key/value pairs inserted. (Note that deletes are also 'inserted')");
    tServerList.addSortableColumn("Query", new NumberType<Long>(), "The number of key/value pairs returned to clients. (Not the number of scans)");
    tServerList.addSortableColumn("Hold&nbsp;Time", new DurationType(), "The amount of time ingest is suspended waiting for data to be written to disk.");
    tServerList
        .addSortableColumn(
            "Minor<br />Compactions",
            new CompactionsType("minor"),
            "The number of minor compactions running and (queued waiting for resources). Minor compactions are the operations where entries are flushed from memory to disk.");
    tServerList.addSortableColumn("Major<br />Compactions", new CompactionsType("major"),
        "The number of major compactions running and (queued waiting for resources). "
            + "Major compactions are the operations where many smaller files are grouped into a larger file, eliminating duplicates and cleaning up deletes.");
    tServerList.addSortableColumn("OS&nbsp;Load", new NumberType<Double>(0., guessHighLoad * 1., 0., guessHighLoad * 3.),
        "The Unix one minute load average. The average number of processes in the run queue over a one minute interval.");
    
    for (TabletServerStatus status : tservers) {
      if (status == null)
        status = NO_STATUS;
      TableInfo summary = Monitor.summarizeTableStats(status);
      TableRow row = tServerList.prepareRow();
      row.add(status); // add for server name
      row.add(summary.tablets);
      row.add(now - status.lastContact);
      row.add(summary.recs);
      row.add(summary.ingestRate);
      row.add(summary.queryRate);
      row.add(status.holdTime);
      row.add(summary); // add for minor compactions
      row.add(summary); // add for major compactions
      row.add(status.osLoad);
      tServerList.addRow(row);
    }
    tServerList.generate(req, sb);
  }
  
}
