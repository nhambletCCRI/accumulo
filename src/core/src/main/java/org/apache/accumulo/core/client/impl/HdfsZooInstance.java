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
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.accumulo.core.util.StringUtil;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.zookeeper.ZooCache;
import org.apache.accumulo.core.zookeeper.ZooLock;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * An implementation of Instance that looks in HDFS and ZooKeeper to find the master and root tablet location.
 * 
 */
public class HdfsZooInstance implements Instance {
  
  public static class AccumuloNotInitializedException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    public AccumuloNotInitializedException(String string) {
      super(string);
    }
  }
  
  private static HdfsZooInstance cachedHdfsZooInstance = null;
  
  public static Instance getInstance() {
    if (cachedHdfsZooInstance == null)
      cachedHdfsZooInstance = new HdfsZooInstance();
    return cachedHdfsZooInstance;
  }
  
  protected HdfsZooInstance() {}
  
  private static ZooCache zooCache = new ZooCache();
  private static String instanceId = null;
  private static final Logger log = Logger.getLogger(HdfsZooInstance.class);
  
  public static String lookupInstanceName(ZooCache zooCache, UUID instanceId) {
    ArgumentChecker.notNull(zooCache, instanceId);
    for (String name : zooCache.getChildren(Constants.ZROOT + Constants.ZINSTANCES)) {
      String instanceNamePath = Constants.ZROOT + Constants.ZINSTANCES + "/" + name;
      UUID iid = UUID.fromString(new String(zooCache.get(instanceNamePath)));
      if (iid.equals(instanceId)) {
        return name;
      }
    }
    return null;
  }
  
  @Override
  public String getRootTabletLocation() {
    String zRootLocPath = ZooUtil.getRoot(this) + Constants.ZROOT_TABLET_LOCATION;
    
    OpTimer opTimer = new OpTimer(log, Level.TRACE).start("Looking up root tablet location in zoocache.");
    
    byte[] loc = zooCache.get(zRootLocPath);
    
    opTimer.stop("Found root tablet at " + (loc == null ? null : new String(loc)) + " in %DURATION%");
    
    if (loc == null) {
      return null;
    }
    
    return new String(loc).split("\\|")[0];
  }
  
  @Override
  public List<String> getMasterLocations() {
    
    String masterLocPath = ZooUtil.getRoot(this) + Constants.ZMASTER_LOCK;
    
    OpTimer opTimer = new OpTimer(log, Level.TRACE).start("Looking up master location in zoocache.");
    
    byte[] loc = ZooLock.getLockData(masterLocPath);
    
    opTimer.stop("Found master at " + (loc == null ? null : new String(loc)) + " in %DURATION%");
    
    if (loc == null) {
      return Collections.emptyList();
    }
    
    return Collections.singletonList(new String(loc));
  }
  
  @Override
  public String getInstanceID() {
    if (instanceId == null)
      _getInstanceID();
    return instanceId;
  }
  
  private static synchronized void _getInstanceID() {
    if (instanceId == null) {
      Configuration conf = new Configuration();
      try {
        FileSystem fs = FileSystem.get(conf);
        
        FileStatus[] files = fs.listStatus(Constants.getInstanceIdLocation());
        if (files == null || files.length == 0) {
          log.error("unable obtain instance id at " + Constants.getInstanceIdLocation());
          throw new AccumuloNotInitializedException("Accumulo not initialized, there is no instance id at " + Constants.getInstanceIdLocation());
        } else if (files.length != 1) {
          log.error("multiple potential instances in " + Constants.getInstanceIdLocation());
          throw new RuntimeException("Accumulo found multiple possible instance ids in " + Constants.getInstanceIdLocation());
        } else {
          instanceId = files[0].getPath().getName();
        }
      } catch (IOException e) {
        throw new RuntimeException("Accumulo not initialized, there is no instance id at " + Constants.getInstanceIdLocation(), e);
      }
    }
  }
  
  @Override
  public String getInstanceName() {
    return lookupInstanceName(zooCache, UUID.fromString(getInstanceID()));
  }
  
  @Override
  public String getZooKeepers() {
    return getConfiguration().get(Property.INSTANCE_ZK_HOST);
  }
  
  @Override
  public int getZooKeepersSessionTimeOut() {
    return (int) getConfiguration().getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT);
  }
  
  @Override
  public Connector getConnector(String user, byte[] pass) throws AccumuloException, AccumuloSecurityException {
    return new ConnectorImpl(this, user, pass);
  }
  
  @Override
  public Connector getConnector(String user, CharSequence pass) throws AccumuloException, AccumuloSecurityException {
    return getConnector(user, TextUtil.getBytes(new Text(pass.toString())));
  }
  
  private AccumuloConfiguration conf = null;
  
  @Override
  public AccumuloConfiguration getConfiguration() {
    if (conf == null)
      conf = AccumuloConfiguration.getSystemConfiguration();
    return conf;
  }
  
  @Override
  public void setConfiguration(AccumuloConfiguration conf) {
    this.conf = conf;
  }
  
  public static void main(String[] args) {
    Instance instance = HdfsZooInstance.getInstance();
    System.out.println("Instance Name: " + instance.getInstanceName());
    System.out.println("Instance ID: " + instance.getInstanceID());
    System.out.println("ZooKeepers: " + instance.getZooKeepers());
    System.out.println("Masters: " + StringUtil.join(instance.getMasterLocations(), ", "));
  }
}
