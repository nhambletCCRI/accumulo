/**
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
package org.apache.accumulo.server.tabletserver.log;

import static org.apache.accumulo.server.logger.LogEvents.COMPACTION_FINISH;
import static org.apache.accumulo.server.logger.LogEvents.COMPACTION_START;
import static org.apache.accumulo.server.logger.LogEvents.DEFINE_TABLET;
import static org.apache.accumulo.server.logger.LogEvents.MANY_MUTATIONS;
import static org.apache.accumulo.server.logger.LogEvents.OPEN;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.tabletserver.thrift.LogCopyInfo;
import org.apache.accumulo.core.tabletserver.thrift.LoggerClosedException;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchLogIDException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Iface;
import org.apache.accumulo.core.tabletserver.thrift.TabletMutations;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.server.logger.LogFileKey;
import org.apache.accumulo.server.logger.LogFileValue;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.accumulo.server.tabletserver.log.RemoteLogger.LogWork;
import org.apache.accumulo.server.tabletserver.log.RemoteLogger.LoggerOperation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

/**
 * Wrap a connection to a logger.
 * 
 */
public class DfsLogger implements IRemoteLogger {
  private static Logger log = Logger.getLogger(DfsLogger.class);
  
  public interface ServerConfig {
    AccumuloConfiguration getConfiguration();
    
    FileSystem getFileSystem();
    
    Set<TServerInstance> getCurrentTServers();
  }

  private LinkedBlockingQueue<LogWork> workQueue = new LinkedBlockingQueue<LogWork>();
  
  private String closeLock = new String("foo");
  
  private static final LogWork CLOSED_MARKER = new LogWork(null, null);
  
  private static final LogFileValue EMPTY = new LogFileValue();
  
  private boolean closed = false;

  private class LogWriterTask implements Runnable {

    @Override
    public void run() {
      ArrayList<LogWork> work = new ArrayList<LogWork>();
      while (true) {
        work.clear();
        
        try {
          work.add(workQueue.take());
        } catch (InterruptedException ex) {
          continue;
        }
        workQueue.drainTo(work);
        
        synchronized (closeLock) {
          if (!closed) {
            try {
              logFile.flush();
              logFile.sync();
            } catch (IOException ex) {
              log.warn("Exception syncing " + ex);
              for (LogWork logWork : work) {
                logWork.exception = ex;
              }
            }
          } else {
            for (LogWork logWork : work) {
              logWork.exception = new LoggerClosedException();
            }
          }
        }
        
        boolean sawClosedMarker = false;
        for (LogWork logWork : work)
          if (logWork == CLOSED_MARKER)
            sawClosedMarker = true;
          else
            logWork.latch.countDown();
        
        if (sawClosedMarker) {
          synchronized (closeLock) {
            closeLock.notifyAll();
          }
          break;
        }
      }
    }
  }

  /* (non-Javadoc)
   * @see org.apache.accumulo.server.tabletserver.log.IRemoteLogger#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    // filename is unique
    if (obj == null)
      return false;
    if (obj instanceof IRemoteLogger)
      return getFileName().equals(((IRemoteLogger) obj).getFileName());
    return false;
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.tabletserver.log.IRemoteLogger#hashCode()
   */
  @Override
  public int hashCode() {
    // filename is unique
    return getFileName().hashCode();
  }
  
  private ServerConfig conf;
  private FSDataOutputStream logFile;
  private Path logPath;
  
  public DfsLogger(ServerConfig conf) throws IOException {
    this.conf = conf;
  }
  
  public DfsLogger(ServerConfig conf, String filename) throws IOException {
    this.conf = conf;
    this.logPath = new Path(Constants.getWalDirectory(conf.getConfiguration()), filename);
  }

  public synchronized void open() throws IOException {
    String filename = UUID.randomUUID().toString();

    logPath = new Path(Constants.getWalDirectory(conf.getConfiguration()), filename);
    try {
      FileSystem fs = conf.getFileSystem();
      short replication = (short) conf.getConfiguration().getCount(Property.TSERV_WAL_REPLICATION);
      if (replication == 0)
        replication = (short) fs.getDefaultReplication();
      long blockSize = conf.getConfiguration().getMemoryInBytes(Property.TSERV_WAL_BLOCKSIZE);
      if (blockSize == 0)
        blockSize = (long) (conf.getConfiguration().getMemoryInBytes(Property.TSERV_WALOG_MAX_SIZE) * 1.1);
      int checkSum = fs.getConf().getInt("io.bytes.per.checksum", 512);
      blockSize -= blockSize % checkSum;
      blockSize = Math.max(blockSize, checkSum);
      logFile = fs.create(logPath, true, fs.getConf().getInt("io.file.buffer.size", 4096), replication, blockSize);
      LogFileKey key = new LogFileKey();
      key.event = OPEN;
      key.tserverSession = filename;
      key.filename = filename;
      write(key, EMPTY);
      log.debug("Got new write-ahead log: " + this);
    } catch (IOException ex) {
      if (logFile != null)
        logFile.close();
      logFile = null;
      throw ex;
    }
    
    Thread t = new Daemon(new LogWriterTask());
    t.setName("Accumulo WALog thread " + toString());
    t.start();
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.tabletserver.log.IRemoteLogger#toString()
   */
  @Override
  public String toString() {
    return getLogger() + "/" + getFileName();
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.tabletserver.log.IRemoteLogger#getLogger()
   */
  @Override
  public String getLogger() {
    return "";
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.tabletserver.log.IRemoteLogger#getFileName()
   */
  @Override
  public String getFileName() {
    return logPath.getName();
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.tabletserver.log.IRemoteLogger#close()
   */
  @Override
  public void close() throws NoSuchLogIDException, LoggerClosedException, TException {
    
    synchronized (closeLock) {
      if (closed)
        return;
      // after closed is set to true, nothing else should be added to the queue
      // CLOSED_MARKER should be the last thing on the queue, therefore when the
      // background thread sees the marker and exits there should be nothing else
      // to process... so nothing should be left waiting for the background
      // thread to do work
      closed = true;
      workQueue.add(CLOSED_MARKER);
      while (!workQueue.isEmpty())
        try {
          closeLock.wait();
        } catch (InterruptedException e) {
          log.info("Interrupted");
        }
    }

    if (logFile != null)
      try {
        logFile.close();
      } catch (IOException ex) {
        log.error(ex);
        throw new LoggerClosedException();
      }
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.tabletserver.log.IRemoteLogger#defineTablet(int, int, org.apache.accumulo.core.data.KeyExtent)
   */
  @Override
  public synchronized void defineTablet(int seq, int tid, KeyExtent tablet) throws NoSuchLogIDException, LoggerClosedException, TException {
    // write this log to the METADATA table
    final LogFileKey key = new LogFileKey();
    key.event = DEFINE_TABLET;
    key.seq = seq;
    key.tid = tid;
    key.tablet = tablet;
    try {
      write(key, EMPTY);
      logFile.flush();
      logFile.sync();
    } catch (IOException ex) {
      log.error(ex);
      throw new LoggerClosedException();
    }
  }
  
  /**
   * @param key
   * @param empty2
   * @throws IOException
   */
  private synchronized void write(LogFileKey key, LogFileValue value) throws IOException {
    key.write(logFile);
    value.write(logFile);
  }

  /* (non-Javadoc)
   * @see org.apache.accumulo.server.tabletserver.log.IRemoteLogger#log(int, int, org.apache.accumulo.core.data.Mutation)
   */
  @Override
  public LoggerOperation log(int seq, int tid, Mutation mutation) throws NoSuchLogIDException, LoggerClosedException, TException {
    return logManyTablets(Collections.singletonList(new TabletMutations(tid, seq, Collections.singletonList(mutation.toThrift()))));
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.tabletserver.log.IRemoteLogger#logManyTablets(java.util.List)
   */
  @Override
  public LoggerOperation logManyTablets(List<TabletMutations> mutations) throws NoSuchLogIDException, LoggerClosedException, TException {
    LogWork work = new LogWork(mutations, new CountDownLatch(1));
    
    synchronized (DfsLogger.this) {
      try {
        for (TabletMutations mutation : mutations) {
          LogFileKey key = new LogFileKey();
          key.event = MANY_MUTATIONS;
          key.seq = mutation.seq;
          key.tid = mutation.tabletID;
          LogFileValue value = new LogFileValue();
          Mutation[] m = new Mutation[mutation.mutations.size()];
          for (int i = 0; i < m.length; i++)
            m[i] = new Mutation(mutation.mutations.get(i));
          value.mutations = m;
          write(key, value);
        }
      } catch (Exception e) {
        log.error(e, e);
        work.exception = e;
      }
    }

    synchronized (closeLock) {
      // use a different lock for close check so that adding to work queue does not need
      // to wait on walog I/O operations

      if (closed)
        throw new LoggerClosedException();
      workQueue.add(work);
    }

    return new LoggerOperation(work);
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.tabletserver.log.IRemoteLogger#minorCompactionFinished(int, int, java.lang.String)
   */
  @Override
  public synchronized void minorCompactionFinished(int seq, int tid, String fqfn) throws NoSuchLogIDException, LoggerClosedException, TException {
    LogFileKey key = new LogFileKey();
    key.event = COMPACTION_FINISH;
    key.seq = seq;
    key.tid = tid;
    try {
      write(key, EMPTY);
    } catch (IOException ex) {
      log.error(ex);
      throw new LoggerClosedException();
    }
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.tabletserver.log.IRemoteLogger#minorCompactionStarted(int, int, java.lang.String)
   */
  @Override
  public synchronized void minorCompactionStarted(int seq, int tid, String fqfn) throws NoSuchLogIDException, LoggerClosedException, TException {
    LogFileKey key = new LogFileKey();
    key.event = COMPACTION_START;
    key.seq = seq;
    key.tid = tid;
    key.filename = fqfn;
    try {
      write(key, EMPTY);
    } catch (IOException ex) {
      log.error(ex);
      throw new LoggerClosedException();
    }
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.tabletserver.log.IRemoteLogger#startCopy(java.lang.String, java.lang.String)
   */
  @Override
  public synchronized LogCopyInfo startCopy(String source, String dest) throws ThriftSecurityException, TException {
    Iface client = null;
    try {
      Set<TServerInstance> current = conf.getCurrentTServers();
      if (current.isEmpty())
        throw new RuntimeException("No servers for recovery");
      Random random = new Random();
      int choice = random.nextInt(current.size());
      TServerInstance instance = current.toArray(new TServerInstance[] {})[choice];
      client = ThriftUtil.getTServerClient(instance.hostPort(), conf.getConfiguration());
      log.info("Asking " + instance.hostPort() + " to copy/sort from " + source + " to " + dest);
      LogCopyInfo result = new LogCopyInfo();
      client.sortLog(null, SecurityConstants.getSystemCredentials(), source, dest);
      result.fileSize = conf.getConfiguration().getMemoryInBytes(Property.TSERV_WALOG_MAX_SIZE);
      return result;
    } finally {
      if (client != null)
        ThriftUtil.returnClient(client);
    }
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.tabletserver.log.IRemoteLogger#getClosedLogs()
   */
  @Override
  public synchronized List<String> getClosedLogs() throws ThriftSecurityException, TException {
    return Collections.emptyList();
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.tabletserver.log.IRemoteLogger#removeFile(java.util.List)
   */
  @Override
  public synchronized void removeFile(List<String> files) throws ThriftSecurityException, TException {
  }
  
}