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

import java.security.SecurityPermission;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.curator.CuratorCaches;
import org.apache.accumulo.fate.curator.CuratorUtil;
import org.apache.curator.framework.recipes.cache.ChildData;

public class Tables {
  private static SecurityPermission TABLES_PERMISSION = new SecurityPermission("tablesPermission");
  
  //TODO fix code base for removed Table.clearCache method
  
  private static CuratorCaches getZooCache(Instance instance) {
    SecurityManager sm = System.getSecurityManager();
    if (sm != null) {
      sm.checkPermission(TABLES_PERMISSION);
    }
    return CuratorCaches.getInstance(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut());
  }
  
  private static SortedMap<String,String> getMap(Instance instance, boolean nameAsKey) {
    CuratorCaches zc = getZooCache(instance);
    
    List<ChildData> tableIds = zc.getChildren(ZooUtil.getRoot(instance) + Constants.ZTABLES);
    
    TreeMap<String,String> tableMap = new TreeMap<String,String>();
    
    for (ChildData tableId : tableIds) {
      ChildData namePath = zc.get(tableId.getPath() + Constants.ZTABLE_NAME);
      if (namePath != null) {
        if (nameAsKey)
          tableMap.put(new String(namePath.getData()), CuratorUtil.getNodeName(tableId));
        else
          tableMap.put(CuratorUtil.getNodeName(tableId), new String(namePath.getData()));
      }
    }
    
    return tableMap;
  }
  
  public static String getTableId(Instance instance, String tableName) throws TableNotFoundException {
    String tableId = getNameToIdMap(instance).get(tableName);
    if (tableId == null)
      throw new TableNotFoundException(tableId, tableName, null);
    return tableId;
  }
  
  public static String getTableName(Instance instance, String tableId) throws TableNotFoundException {
    String tableName = getIdToNameMap(instance).get(tableId);
    if (tableName == null)
      throw new TableNotFoundException(tableId, tableName, null);
    return tableName;
  }
  
  public static SortedMap<String,String> getNameToIdMap(Instance instance) {
    return getMap(instance, true);
  }
  
  public static SortedMap<String,String> getIdToNameMap(Instance instance) {
    return getMap(instance, false);
  }
  
  public static boolean exists(Instance instance, String tableId) {
    CuratorCaches zc = getZooCache(instance);
    ChildData table = zc.get(ZooUtil.getRoot(instance) + Constants.ZTABLES + '/' + tableId);
    return table != null;
  }
  
  public static String getPrintableTableNameFromId(Map<String,String> tidToNameMap, String tableId) {
    String tableName = tidToNameMap.get(tableId);
    return tableName == null ? "(ID:" + tableId + ")" : tableName;
  }
  
  public static String getPrintableTableIdFromName(Map<String,String> nameToIdMap, String tableName) {
    String tableId = nameToIdMap.get(tableName);
    return tableId == null ? "(NAME:" + tableName + ")" : tableId;
  }
  
  public static String getPrintableTableInfoFromId(Instance instance, String tableId) {
    String tableName = null;
    try {
      tableName = getTableName(instance, tableId);
    } catch (TableNotFoundException e) {
      // handled in the string formatting
    }
    return tableName == null ? String.format("?(ID:%s)", tableId) : String.format("%s(ID:%s)", tableName, tableId);
  }
  
  public static String getPrintableTableInfoFromName(Instance instance, String tableName) {
    String tableId = null;
    try {
      tableId = getTableId(instance, tableName);
    } catch (TableNotFoundException e) {
      // handled in the string formatting
    }
    return tableId == null ? String.format("%s(?)", tableName) : String.format("%s(ID:%s)", tableName, tableId);
  }
  
  public static TableState getTableState(Instance instance, String tableId) {
    String statePath = ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_STATE;
    CuratorCaches zc = getZooCache(instance);
    byte[] state = zc.get(statePath).getData();
    if (state == null)
      return TableState.UNKNOWN;
    
    return TableState.valueOf(new String(state));
  }
}
