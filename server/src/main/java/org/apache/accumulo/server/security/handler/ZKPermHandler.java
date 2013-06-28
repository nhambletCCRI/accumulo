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
package org.apache.accumulo.server.security.handler;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.util.MetadataTable;
import org.apache.accumulo.core.util.RootTable;
import org.apache.accumulo.fate.curator.CuratorReaderWriter.NodeExistsPolicy;
import org.apache.accumulo.server.curator.CuratorReaderWriter;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;

/**
 * 
 */
public class ZKPermHandler implements PermissionHandler {
  private static final Logger log = Logger.getLogger(ZKAuthorizor.class);
  private static PermissionHandler zkPermHandlerInstance = null;
  
  private String ZKUserPath;
  private String ZKTablePath;
  private final ZooCache zooCache;
  private final String ZKUserSysPerms = "/System";
  private final String ZKUserTablePerms = "/Tables";
  
  public static synchronized PermissionHandler getInstance() {
    if (zkPermHandlerInstance == null)
      zkPermHandlerInstance = new ZKPermHandler();
    return zkPermHandlerInstance;
  }
  
  @Override
  public void initialize(String instanceId, boolean initialize) {
    ZKUserPath = ZKSecurityTool.getInstancePath(instanceId) + "/users";
    ZKTablePath = ZKSecurityTool.getInstancePath(instanceId) + "/tables";
  }
  
  public ZKPermHandler() {
    zooCache = new ZooCache();
  }
  
  @Override
  public boolean hasTablePermission(String user, String table, TablePermission permission) throws TableNotFoundException {
    byte[] serializedPerms;
    try {
      String path = ZKUserPath + "/" + user + ZKUserTablePerms + "/" + table;
      CuratorReaderWriter.getInstance().sync(path);
      serializedPerms = CuratorReaderWriter.getInstance().getData(path, null);
    } catch (KeeperException e) {
      if (e.code() == Code.NONODE) {
        // maybe the table was just deleted?
        try {
          // check for existence:
          CuratorReaderWriter.getInstance().getData(ZKTablePath + "/" + table, null);
          // it's there, you don't have permission
          return false;
        } catch (InterruptedException ex) {
          log.warn("Unhandled InterruptedException, failing closed for table permission check", e);
          return false;
        } catch (KeeperException ex) {
          // not there, throw an informative exception
          if (e.code() == Code.NONODE) {
            throw new TableNotFoundException(null, table, "while checking permissions");
          }
          log.warn("Unhandled InterruptedException, failing closed for table permission check", e);
        }
        return false;
      }
      log.warn("Unhandled KeeperException, failing closed for table permission check", e);
      return false;
    } catch (InterruptedException e) {
      log.warn("Unhandled InterruptedException, failing closed for table permission check", e);
      return false;
    }
    if (serializedPerms != null) {
      return ZKSecurityTool.convertTablePermissions(serializedPerms).contains(permission);
    }
    return false;
  }
  
  @Override
  public boolean hasCachedTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException {
    ChildData serializedPerms = zooCache.get(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + table);
    if (serializedPerms != null) {
      return ZKSecurityTool.convertTablePermissions(serializedPerms.getData()).contains(permission);
    }
    return false;
  }
  
  @Override
  public void grantSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException {
    try {
      ChildData permBytes = zooCache.get(ZKUserPath + "/" + user + ZKUserSysPerms);
      Set<SystemPermission> perms;
      if (permBytes == null) {
        perms = new TreeSet<SystemPermission>();
      } else {
        perms = ZKSecurityTool.convertSystemPermissions(permBytes.getData());
      }
      
      if (perms.add(permission)) {
        synchronized (zooCache) {
          zooCache.clear();
          CuratorReaderWriter.getInstance().putPersistentData(ZKUserPath + "/" + user + ZKUserSysPerms, ZKSecurityTool.convertSystemPermissions(perms),
              NodeExistsPolicy.OVERWRITE);
        }
      }
    } catch (KeeperException e) {
      log.error(e, e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error(e, e);
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public void grantTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException {
    Set<TablePermission> tablePerms;
    ChildData serializedPerms = zooCache.get(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + table);
    if (serializedPerms != null)
      tablePerms = ZKSecurityTool.convertTablePermissions(serializedPerms.getData());
    else
      tablePerms = new TreeSet<TablePermission>();
    
    try {
      if (tablePerms.add(permission)) {
        synchronized (zooCache) {
          zooCache.clear(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + table);
          CuratorReaderWriter zoo = CuratorReaderWriter.getInstance();
          zoo.putPersistentData(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + table, ZKSecurityTool.convertTablePermissions(tablePerms),
              NodeExistsPolicy.OVERWRITE);
        }
      }
    } catch (KeeperException e) {
      log.error(e, e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error(e, e);
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public void revokeSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException {
    ChildData sysPermBytes = zooCache.get(ZKUserPath + "/" + user + ZKUserSysPerms);
    
    // User had no system permission, nothing to revoke.
    if (sysPermBytes == null)
      return;
    
    Set<SystemPermission> sysPerms = ZKSecurityTool.convertSystemPermissions(sysPermBytes.getData());
    
    try {
      if (sysPerms.remove(permission)) {
        synchronized (zooCache) {
          zooCache.clear();
          CuratorReaderWriter.getInstance().putPersistentData(ZKUserPath + "/" + user + ZKUserSysPerms, ZKSecurityTool.convertSystemPermissions(sysPerms),
              NodeExistsPolicy.OVERWRITE);
        }
      }
    } catch (KeeperException e) {
      log.error(e, e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error(e, e);
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public void revokeTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException {
    ChildData serializedPerms = zooCache.get(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + table);
    
    // User had no table permission, nothing to revoke.
    if (serializedPerms == null)
      return;
    
    Set<TablePermission> tablePerms = ZKSecurityTool.convertTablePermissions(serializedPerms.getData());
    try {
      if (tablePerms.remove(permission)) {
        zooCache.clear();
        CuratorReaderWriter zoo = CuratorReaderWriter.getInstance();
        if (tablePerms.size() == 0)
          zoo.recursiveDelete(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + table);
        else
          zoo.putPersistentData(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + table, ZKSecurityTool.convertTablePermissions(tablePerms),
              NodeExistsPolicy.OVERWRITE);
      }
    } catch (KeeperException e) {
      log.error(e, e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error(e, e);
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public void cleanTablePermissions(String table) throws AccumuloSecurityException {
    try {
      synchronized (zooCache) {
        zooCache.clear();
        CuratorReaderWriter zoo = CuratorReaderWriter.getInstance();
        for (String user : zooCache.getChildKeys(ZKUserPath))
          zoo.recursiveDelete(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + table);
      }
    } catch (KeeperException e) {
      log.error(e, e);
      throw new AccumuloSecurityException("unknownUser", SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error(e, e);
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public void initializeSecurity(TCredentials itw, String rootuser) throws AccumuloSecurityException {
    CuratorReaderWriter zoo = CuratorReaderWriter.getInstance();
    
    // create the root user with all system privileges, no table privileges, and no record-level authorizations
    Set<SystemPermission> rootPerms = new TreeSet<SystemPermission>();
    for (SystemPermission p : SystemPermission.values())
      rootPerms.add(p);
    Map<String,Set<TablePermission>> tablePerms = new HashMap<String,Set<TablePermission>>();
    // Allow the root user to flush the system tables
    tablePerms.put(RootTable.ID, Collections.singleton(TablePermission.ALTER_TABLE));
    tablePerms.put(MetadataTable.ID, Collections.singleton(TablePermission.ALTER_TABLE));
    
    try {
      // prep parent node of users with root username
      if (!zoo.exists(ZKUserPath))
        zoo.putPersistentData(ZKUserPath, rootuser.getBytes(), NodeExistsPolicy.FAIL);
      
      initUser(rootuser);
      zoo.putPersistentData(ZKUserPath + "/" + rootuser + ZKUserSysPerms, ZKSecurityTool.convertSystemPermissions(rootPerms), NodeExistsPolicy.FAIL);
      for (Entry<String,Set<TablePermission>> entry : tablePerms.entrySet())
        createTablePerm(rootuser, entry.getKey(), entry.getValue());
    } catch (KeeperException e) {
      log.error(e, e);
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      log.error(e, e);
      throw new RuntimeException(e);
    }
  }
  
  /**
   * @param user
   * @throws AccumuloSecurityException
   */
  @Override
  public void initUser(String user) throws AccumuloSecurityException {
    CuratorReaderWriter zoo = CuratorReaderWriter.getInstance();
    try {
      zoo.putPersistentData(ZKUserPath + "/" + user, new byte[0], NodeExistsPolicy.SKIP);
      zoo.putPersistentData(ZKUserPath + "/" + user + ZKUserTablePerms, new byte[0], NodeExistsPolicy.SKIP);
    } catch (KeeperException e) {
      log.error(e, e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error(e, e);
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Sets up a new table configuration for the provided user/table. No checking for existence is done here, it should be done before calling.
   */
  private void createTablePerm(String user, String table, Set<TablePermission> perms) throws KeeperException, InterruptedException {
    synchronized (zooCache) {
      zooCache.clear();
      CuratorReaderWriter.getInstance().putPersistentData(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + table,
          ZKSecurityTool.convertTablePermissions(perms), NodeExistsPolicy.FAIL);
    }
  }
  
  @Override
  public void cleanUser(String user) throws AccumuloSecurityException {
    try {
      synchronized (zooCache) {
        CuratorReaderWriter zoo = CuratorReaderWriter.getInstance();
        zoo.recursiveDelete(ZKUserPath + "/" + user + ZKUserSysPerms);
        zoo.recursiveDelete(ZKUserPath + "/" + user + ZKUserTablePerms);
        zooCache.clear(ZKUserPath + "/" + user);
      }
    } catch (InterruptedException e) {
      log.error(e, e);
      throw new RuntimeException(e);
    } catch (KeeperException e) {
      log.error(e, e);
      if (e.code().equals(KeeperException.Code.NONODE))
        throw new AccumuloSecurityException(user, SecurityErrorCode.USER_DOESNT_EXIST, e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
      
    }
  }
  
  @Override
  public boolean hasSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException {
    byte[] perms;
    try {
      String path = ZKUserPath + "/" + user + ZKUserSysPerms;
      CuratorReaderWriter.getInstance().sync(path);
      perms = CuratorReaderWriter.getInstance().getData(path, null);
    } catch (KeeperException e) {
      if (e.code() == Code.NONODE) {
        return false;
      }
      log.warn("Unhandled KeeperException, failing closed for table permission check", e);
      return false;
    } catch (InterruptedException e) {
      log.warn("Unhandled InterruptedException, failing closed for table permission check", e);
      return false;
    }
    
    if (perms == null)
      return false;
    return ZKSecurityTool.convertSystemPermissions(perms).contains(permission);
  }
  
  @Override
  public boolean hasCachedSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException {
    ChildData perms = zooCache.get(ZKUserPath + "/" + user + ZKUserSysPerms);
    if (perms == null)
      return false;
    return ZKSecurityTool.convertSystemPermissions(perms.getData()).contains(permission);
  }
  
  @Override
  public boolean validSecurityHandlers(Authenticator authent, Authorizor author) {
    return true;
  }
  
  @Override
  public void initTable(String table) throws AccumuloSecurityException {
    // All proper housekeeping is done on delete and permission granting, no work needs to be done here
  }
}
