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
package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests that Accumulo is outputting audit messages as expected. Since this is using MiniAccumuloCluster, it could take a while if we test everything in
 * isolation. We test blocks of related operations, run the whole test in one MiniAccumulo instance, trying to clean up objects between each test. The
 * MiniAccumuloClusterTest sets up the log4j stuff differently to an installed instance, instead piping everything through stdout and writing to a set location
 * so we have to find the logs and grep the bits we need out.
 */
public class AuditMessageTest {
  
  private static MiniAccumuloCluster accumulo;
  private static File logDir;
  private static List<MiniAccumuloCluster.LogWriter> logWriters;
  private final String AUDIT_USER_1 = "AuditUser1";
  private final String AUDIT_USER_2 = "AuditUser2";
  private final PasswordToken PASSWORD_TOKEN = new PasswordToken("password");
  private final String OLD_TEST_TABLE_NAME = "apples";
  private final String NEW_TEST_TABLE_NAME = "oranges";
  private final String THIRD_TEST_TABLE_NAME = "pears";
  private final Authorizations auths = new Authorizations("private", "public");
  private static TemporaryFolder folder = new TemporaryFolder();
  
  // Must be static to survive Junit re-initialising the class every time.
  private static String lastAuditTimestamp;
  private Connector auditConnector;
  private Connector conn;
  
  private static ArrayList<String> findAuditMessage(ArrayList<String> input, String pattern) {
    ArrayList<String> result = new ArrayList<String>();
    for (String s : input) {
      if (s.matches(".*" + pattern + ".*"))
        result.add(s);
    }
    return result;
  }
  
  @BeforeClass
  public static void setupMiniCluster() throws Exception {
    folder.create();
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);
    
    accumulo = new MiniAccumuloCluster(folder.getRoot(), "superSecret");
    accumulo.start();
    logDir = accumulo.getConfig().getLogDir();
    logWriters = accumulo.getLogWriters();
  }
  
  /**
   * Returns a List of Audit messages that have been grep'd out of the MiniAccumuloCluster output.
   * 
   * @param stepName
   *          A unique name for the test being executed, to identify the System.out messages.
   * @return A List of the Audit messages, sorted (so in chronological order).
   */
  private ArrayList<String> getAuditMessages(String stepName) throws IOException {
    
    for (MiniAccumuloCluster.LogWriter lw : logWriters) {
      lw.flush();
    }
    
    // Grab the audit messages
    System.out.println("Start of captured audit messages for step " + stepName);
    
    ArrayList<String> result = new ArrayList<String>();
    for (File file : logDir.listFiles()) {
      // We want to grab the files called .out
      if (file.getName().contains(".out") && file.isFile() && file.canRead()) {
        LineIterator it = FileUtils.lineIterator(file, "UTF-8");
        try {
          while (it.hasNext()) {
            String line = it.nextLine();
            if (line.matches(".* \\[" + AuditedSecurityOperation.AUDITLOG + "\\s*\\].*")) {
              // Only include the message if startTimestamp is null. or the message occurred after the startTimestamp value
              if ((lastAuditTimestamp == null) || (line.substring(0, 23).compareTo(lastAuditTimestamp) > 0))
                result.add(line);
            }
          }
        } finally {
          LineIterator.closeQuietly(it);
        }
      }
    }
    Collections.sort(result);
    
    for (String s : result) {
      System.out.println(s);
    }
    System.out.println("End of captured audit messages for step " + stepName);
    if (result.size() > 0)
      lastAuditTimestamp = (result.get(result.size() - 1)).substring(0, 23);
    
    return result;
  }
  
  private void grantEverySystemPriv(Connector conn, String user) throws AccumuloSecurityException, AccumuloException {
    SystemPermission[] arrayOfP = new SystemPermission[] {SystemPermission.SYSTEM, SystemPermission.ALTER_TABLE, SystemPermission.ALTER_USER,
        SystemPermission.CREATE_TABLE, SystemPermission.CREATE_USER, SystemPermission.DROP_TABLE, SystemPermission.DROP_USER};
    for (SystemPermission p : arrayOfP) {
      conn.securityOperations().grantSystemPermission(user, p);
    }
  }
  
  @Before
  public void setup() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, IOException {
    conn = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers()).getConnector("root", new PasswordToken("superSecret"));
    
    // I don't want to recreate the instance for every test since it will take ages.
    // If we run every test as non-root users, I can drop these users every test which should effectively
    // reset the environment.
    
    if (conn.securityOperations().listLocalUsers().contains(AUDIT_USER_1))
      conn.securityOperations().dropLocalUser(AUDIT_USER_1);
    if (conn.securityOperations().listLocalUsers().contains(AUDIT_USER_2))
      conn.securityOperations().dropLocalUser(AUDIT_USER_2);
    if (conn.securityOperations().listLocalUsers().contains(AUDIT_USER_2))
      conn.securityOperations().dropLocalUser(THIRD_TEST_TABLE_NAME);
    if (conn.tableOperations().exists(NEW_TEST_TABLE_NAME))
      conn.tableOperations().delete(NEW_TEST_TABLE_NAME);
    if (conn.tableOperations().exists(OLD_TEST_TABLE_NAME))
      conn.tableOperations().delete(OLD_TEST_TABLE_NAME);
    
    // This will set the lastAuditTimestamp for the first test
    getAuditMessages("setup");
    
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void testTableOperationsAudits() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException, IOException,
      InterruptedException {
    
    conn.securityOperations().createLocalUser(AUDIT_USER_1, PASSWORD_TOKEN);
    conn.securityOperations().grantSystemPermission(AUDIT_USER_1, SystemPermission.SYSTEM);
    conn.securityOperations().grantSystemPermission(AUDIT_USER_1, SystemPermission.CREATE_TABLE);
    
    // Connect as Audit User and do a bunch of stuff.
    // Testing activity begins here
    auditConnector = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers()).getConnector(AUDIT_USER_1, PASSWORD_TOKEN);
    auditConnector.tableOperations().create(OLD_TEST_TABLE_NAME);
    auditConnector.tableOperations().rename(OLD_TEST_TABLE_NAME, NEW_TEST_TABLE_NAME);
    auditConnector.tableOperations().clone(NEW_TEST_TABLE_NAME, OLD_TEST_TABLE_NAME, true, Collections.EMPTY_MAP, Collections.EMPTY_SET);
    auditConnector.tableOperations().delete(OLD_TEST_TABLE_NAME);
    auditConnector.tableOperations().offline(NEW_TEST_TABLE_NAME);
    auditConnector.tableOperations().delete(NEW_TEST_TABLE_NAME);
    // Testing activity ends here
    
    ArrayList<String> auditMessages = getAuditMessages("testTableOperationsAudits");
    
    assertEquals(1, findAuditMessage(auditMessages, "action: createTable; targetTable: " + OLD_TEST_TABLE_NAME).size());
    assertEquals(1, findAuditMessage(auditMessages, "action: renameTable; targetTable: " + OLD_TEST_TABLE_NAME).size());
    assertEquals(1, findAuditMessage(auditMessages, "action: cloneTable; targetTable: " + NEW_TEST_TABLE_NAME).size());
    assertEquals(1, findAuditMessage(auditMessages, "action: deleteTable; targetTable: " + OLD_TEST_TABLE_NAME).size());
    assertEquals(1, findAuditMessage(auditMessages, "action: offlineTable; targetTable: " + NEW_TEST_TABLE_NAME).size());
    assertEquals(1, findAuditMessage(auditMessages, "action: deleteTable; targetTable: " + NEW_TEST_TABLE_NAME).size());
    
  }
  
  @Test
  public void testUserOperationsAudits() throws AccumuloSecurityException, AccumuloException, TableExistsException, InterruptedException, IOException {
    
    conn.securityOperations().createLocalUser(AUDIT_USER_1, PASSWORD_TOKEN);
    conn.securityOperations().grantSystemPermission(AUDIT_USER_1, SystemPermission.SYSTEM);
    conn.securityOperations().grantSystemPermission(AUDIT_USER_1, SystemPermission.CREATE_USER);
    grantEverySystemPriv(conn, AUDIT_USER_1);
    
    // Connect as Audit User and do a bunch of stuff.
    // Start testing activities here
    auditConnector = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers()).getConnector(AUDIT_USER_1, PASSWORD_TOKEN);
    auditConnector.securityOperations().createLocalUser(AUDIT_USER_2, PASSWORD_TOKEN);
    
    // It seems only root can grant stuff.
    conn.securityOperations().grantSystemPermission(AUDIT_USER_2, SystemPermission.ALTER_TABLE);
    conn.securityOperations().revokeSystemPermission(AUDIT_USER_2, SystemPermission.ALTER_TABLE);
    auditConnector.tableOperations().create(NEW_TEST_TABLE_NAME);
    conn.securityOperations().grantTablePermission(AUDIT_USER_2, NEW_TEST_TABLE_NAME, TablePermission.READ);
    conn.securityOperations().revokeTablePermission(AUDIT_USER_2, NEW_TEST_TABLE_NAME, TablePermission.READ);
    auditConnector.securityOperations().changeLocalUserPassword(AUDIT_USER_2, new PasswordToken("anything"));
    auditConnector.securityOperations().changeUserAuthorizations(AUDIT_USER_2, auths);
    auditConnector.securityOperations().dropLocalUser(AUDIT_USER_2);
    // Stop testing activities here
    
    ArrayList<String> auditMessages = getAuditMessages("testUserOperationsAudits");
    
    assertEquals(1, findAuditMessage(auditMessages, "action: createUser; targetUser: " + AUDIT_USER_2).size());
    assertEquals(
        1,
        findAuditMessage(auditMessages,
            "action: grantSystemPermission; permission: " + SystemPermission.ALTER_TABLE.toString() + "; targetUser: " + AUDIT_USER_2).size());
    assertEquals(
        1,
        findAuditMessage(auditMessages,
            "action: revokeSystemPermission; permission: " + SystemPermission.ALTER_TABLE.toString() + "; targetUser: " + AUDIT_USER_2).size());
    assertEquals(
        1,
        findAuditMessage(auditMessages,
            "action: grantTablePermission; permission: " + TablePermission.READ.toString() + "; targetTable: " + NEW_TEST_TABLE_NAME).size());
    assertEquals(
        1,
        findAuditMessage(auditMessages,
            "action: revokeTablePermission; permission: " + TablePermission.READ.toString() + "; targetTable: " + NEW_TEST_TABLE_NAME).size());
    assertEquals(1, findAuditMessage(auditMessages, "action: changePassword; targetUser: " + AUDIT_USER_2 + "").size());
    assertEquals(1, findAuditMessage(auditMessages, "action: changeAuthorizations; targetUser: " + AUDIT_USER_2 + "; authorizations: " + auths.toString())
        .size());
    assertEquals(1, findAuditMessage(auditMessages, "action: dropUser; targetUser: " + AUDIT_USER_2).size());
  }
  
  @Test
  public void testImportExportOperationsAudits() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException,
      IOException, InterruptedException {
    
    conn.securityOperations().createLocalUser(AUDIT_USER_1, PASSWORD_TOKEN);
    conn.securityOperations().grantSystemPermission(AUDIT_USER_1, SystemPermission.SYSTEM);
    conn.securityOperations().changeUserAuthorizations(AUDIT_USER_1, auths);
    grantEverySystemPriv(conn, AUDIT_USER_1);
    
    // Connect as Audit User and do a bunch of stuff.
    // Start testing activities here
    auditConnector = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers()).getConnector(AUDIT_USER_1, PASSWORD_TOKEN);
    auditConnector.tableOperations().create(OLD_TEST_TABLE_NAME);
    
    // Insert some play data
    BatchWriter bw = auditConnector.createBatchWriter(OLD_TEST_TABLE_NAME, new BatchWriterConfig());
    Mutation m = new Mutation("myRow");
    m.put("cf1", "cq1", "v1");
    m.put("cf1", "cq2", "v3");
    bw.addMutation(m);
    bw.close();
    
    // Prepare to export the table
    File exportDir = new File(accumulo.getConfig().getDir().toString() + "/export");
    
    auditConnector.tableOperations().offline(OLD_TEST_TABLE_NAME);
    auditConnector.tableOperations().exportTable(OLD_TEST_TABLE_NAME, exportDir.toString());
    
    // We've exported the table metadata to the MiniAccumuloCluster root dir. Grab the .rf file path to re-import it
    File distCpTxt = new File(exportDir.toString() + "/distcp.txt");
    File importFile = null;
    LineIterator it = FileUtils.lineIterator(distCpTxt, "UTF-8");
    
    // Just grab the first rf file, it will do for now.
    String filePrefix = "file:";
    try {
      while (it.hasNext() && importFile == null) {
        String line = it.nextLine();
        if (line.matches(".*\\.rf")) {
          importFile = new File(line.replaceFirst(filePrefix, ""));
        }
      }
    } finally {
      LineIterator.closeQuietly(it);
    }
    FileUtils.copyFileToDirectory(importFile, exportDir);
    auditConnector.tableOperations().importTable(NEW_TEST_TABLE_NAME, exportDir.toString());
    
    // Now do a Directory (bulk) import of the same data.
    auditConnector.tableOperations().create(THIRD_TEST_TABLE_NAME);
    File failDir = new File(exportDir + "/tmp");
    failDir.mkdirs();
    auditConnector.tableOperations().importDirectory(THIRD_TEST_TABLE_NAME, exportDir.toString(), failDir.toString(), false);
    auditConnector.tableOperations().online(OLD_TEST_TABLE_NAME);
    
    // Stop testing activities here
    
    ArrayList<String> auditMessages = getAuditMessages("testImportExportOperationsAudits");
    
    assertEquals(1, findAuditMessage(auditMessages, String.format(AuditedSecurityOperation.CAN_CREATE_TABLE_AUDIT_TEMPLATE, OLD_TEST_TABLE_NAME)).size());
    assertEquals(1,
        findAuditMessage(auditMessages, String.format(AuditedSecurityOperation.CAN_ONLINE_OFFLINE_TABLE_AUDIT_TEMPLATE, "offlineTable", OLD_TEST_TABLE_NAME))
            .size());
    assertEquals(1,
        findAuditMessage(auditMessages, String.format(AuditedSecurityOperation.CAN_EXPORT_AUDIT_TEMPLATE, OLD_TEST_TABLE_NAME, exportDir.toString())).size());
    assertEquals(1,
        findAuditMessage(auditMessages, String.format(AuditedSecurityOperation.CAN_IMPORT_AUDIT_TEMPLATE, NEW_TEST_TABLE_NAME, exportDir.toString())).size());
    assertEquals(1, findAuditMessage(auditMessages, String.format(AuditedSecurityOperation.CAN_CREATE_TABLE_AUDIT_TEMPLATE, THIRD_TEST_TABLE_NAME)).size());
    assertEquals(
        1,
        findAuditMessage(
            auditMessages,
            String.format(AuditedSecurityOperation.CAN_BULK_IMPORT_AUDIT_TEMPLATE, THIRD_TEST_TABLE_NAME, filePrefix + exportDir.toString(), filePrefix
                + failDir.toString())).size());
    assertEquals(1,
        findAuditMessage(auditMessages, String.format(AuditedSecurityOperation.CAN_ONLINE_OFFLINE_TABLE_AUDIT_TEMPLATE, "onlineTable", OLD_TEST_TABLE_NAME))
            .size());
    
  }
  
  @Test
  public void testDataOperationsAudits() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException, IOException,
      InterruptedException {
    
    conn.securityOperations().createLocalUser(AUDIT_USER_1, PASSWORD_TOKEN);
    conn.securityOperations().grantSystemPermission(AUDIT_USER_1, SystemPermission.SYSTEM);
    conn.securityOperations().changeUserAuthorizations(AUDIT_USER_1, auths);
    grantEverySystemPriv(conn, AUDIT_USER_1);
    
    // Connect as Audit User and do a bunch of stuff.
    // Start testing activities here
    auditConnector = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers()).getConnector(AUDIT_USER_1, PASSWORD_TOKEN);
    auditConnector.tableOperations().create(OLD_TEST_TABLE_NAME);
    
    // Insert some play data
    BatchWriter bw = auditConnector.createBatchWriter(OLD_TEST_TABLE_NAME, new BatchWriterConfig());
    Mutation m = new Mutation("myRow");
    m.put("cf1", "cq1", "v1");
    m.put("cf1", "cq2", "v3");
    bw.addMutation(m);
    bw.close();
    
    // Start testing activities here
    // A regular scan
    Scanner scanner = auditConnector.createScanner(OLD_TEST_TABLE_NAME, auths);
    for (Map.Entry<Key,Value> entry : scanner) {
      System.out.println("Scanner row: " + entry.getKey() + " " + entry.getValue());
    }
    scanner.close();
    
    // A batch scan
    BatchScanner bs = auditConnector.createBatchScanner(OLD_TEST_TABLE_NAME, auths, 1);
    bs.fetchColumn(new Text("cf1"), new Text("cq1"));
    bs.setRanges(Arrays.asList(new Range("myRow", "myRow~")));
    
    for (Map.Entry<Key,Value> entry : bs) {
      System.out.println("BatchScanner row: " + entry.getKey() + " " + entry.getValue());
    }
    bs.close();
    
    // Delete some data.
    auditConnector.tableOperations().deleteRows(OLD_TEST_TABLE_NAME, new Text("myRow"), new Text("myRow~"));
    
    // End of testing activities
    
    ArrayList<String> auditMessages = getAuditMessages("testDataOperationsAudits");
    assertTrue(1 <= findAuditMessage(auditMessages, "action: scan; targetTable: " + OLD_TEST_TABLE_NAME).size());
    assertTrue(1 <= findAuditMessage(auditMessages, "action: scan; targetTable: " + OLD_TEST_TABLE_NAME).size());
    assertEquals(1,
        findAuditMessage(auditMessages, String.format(AuditedSecurityOperation.CAN_DELETE_RANGE_AUDIT_TEMPLATE, OLD_TEST_TABLE_NAME, "myRow", "myRow~")).size());
    
  }
  
  @Test
  public void testDeniedAudits() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException, IOException,
      InterruptedException {
    
    // Create our user with no privs
    conn.securityOperations().createLocalUser(AUDIT_USER_1, PASSWORD_TOKEN);
    conn.tableOperations().create(OLD_TEST_TABLE_NAME);
    auditConnector = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers()).getConnector(AUDIT_USER_1, PASSWORD_TOKEN);
    
    // Start testing activities
    // We should get denied or / failed audit messages here.
    // We don't want the thrown exceptions to stop our tests, and we are not testing that the Exceptions are thrown.
    
    try {
      auditConnector.tableOperations().create(NEW_TEST_TABLE_NAME);
    } catch (AccumuloSecurityException ex) {}
    try {
      auditConnector.tableOperations().rename(OLD_TEST_TABLE_NAME, NEW_TEST_TABLE_NAME);
    } catch (AccumuloSecurityException ex) {}
    try {
      auditConnector.tableOperations().clone(OLD_TEST_TABLE_NAME, NEW_TEST_TABLE_NAME, true, Collections.<String,String> emptyMap(),
          Collections.<String> emptySet());
    } catch (AccumuloSecurityException ex) {}
    try {
      auditConnector.tableOperations().delete(OLD_TEST_TABLE_NAME);
    } catch (AccumuloSecurityException ex) {}
    try {
      auditConnector.tableOperations().offline(OLD_TEST_TABLE_NAME);
    } catch (AccumuloSecurityException ex) {}
    try {
      Scanner scanner = auditConnector.createScanner(OLD_TEST_TABLE_NAME, auths);
      scanner.iterator().next().getKey();
    } catch (RuntimeException ex) {}
    try {
      auditConnector.tableOperations().deleteRows(OLD_TEST_TABLE_NAME, new Text("myRow"), new Text("myRow~"));
    } catch (AccumuloSecurityException ex) {}
    
    // ... that will do for now.
    // End of testing activities
    
    ArrayList<String> auditMessages = getAuditMessages("testDeniedAudits");
    assertEquals(1,
        findAuditMessage(auditMessages, "operation: denied;.*" + String.format(AuditedSecurityOperation.CAN_CREATE_TABLE_AUDIT_TEMPLATE, NEW_TEST_TABLE_NAME))
            .size());
    assertEquals(
        1,
        findAuditMessage(auditMessages,
            "operation: denied;.*" + String.format(AuditedSecurityOperation.CAN_RENAME_TABLE_AUDIT_TEMPLATE, OLD_TEST_TABLE_NAME, NEW_TEST_TABLE_NAME)).size());
    assertEquals(
        1,
        findAuditMessage(auditMessages,
            "operation: denied;.*" + String.format(AuditedSecurityOperation.CAN_CLONE_TABLE_AUDIT_TEMPLATE, OLD_TEST_TABLE_NAME, NEW_TEST_TABLE_NAME)).size());
    assertEquals(1,
        findAuditMessage(auditMessages, "operation: denied;.*" + String.format(AuditedSecurityOperation.CAN_DELETE_TABLE_AUDIT_TEMPLATE, OLD_TEST_TABLE_NAME))
            .size());
    assertEquals(
        1,
        findAuditMessage(auditMessages,
            "operation: denied;.*" + String.format(AuditedSecurityOperation.CAN_ONLINE_OFFLINE_TABLE_AUDIT_TEMPLATE, "offlineTable", OLD_TEST_TABLE_NAME))
            .size());
    assertEquals(1, findAuditMessage(auditMessages, "operation: denied;.*" + "action: scan; targetTable: " + OLD_TEST_TABLE_NAME).size());
    assertEquals(
        1,
        findAuditMessage(auditMessages,
            "operation: denied;.*" + String.format(AuditedSecurityOperation.CAN_DELETE_RANGE_AUDIT_TEMPLATE, OLD_TEST_TABLE_NAME, "myRow", "myRow~")).size());
  }
  
  @Test
  public void testFailedAudits() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException, IOException,
      InterruptedException {
    
    // Start testing activities
    // Test that we get a few "failed" audit messages come through when we tell it to do dumb stuff
    // We don't want the thrown exceptions to stop our tests, and we are not testing that the Exceptions are thrown.
    try {
      conn.securityOperations().dropLocalUser(AUDIT_USER_2);
    } catch (AccumuloSecurityException ex) {}
    try {
      conn.securityOperations().revokeSystemPermission(AUDIT_USER_2, SystemPermission.ALTER_TABLE);
    } catch (AccumuloSecurityException ex) {}
    try {
      conn.securityOperations().createLocalUser("root", new PasswordToken("super secret"));
    } catch (AccumuloSecurityException ex) {}
    ArrayList<String> auditMessages = getAuditMessages("testFailedAudits");
    // ... that will do for now.
    // End of testing activities
    
    assertEquals(1, findAuditMessage(auditMessages, String.format(AuditedSecurityOperation.DROP_USER_AUDIT_TEMPLATE, AUDIT_USER_2)).size());
    assertEquals(
        1,
        findAuditMessage(auditMessages,
            String.format(AuditedSecurityOperation.REVOKE_SYSTEM_PERMISSION_AUDIT_TEMPLATE, SystemPermission.ALTER_TABLE, AUDIT_USER_2)).size());
    assertEquals(1, findAuditMessage(auditMessages, String.format(AuditedSecurityOperation.CREATE_USER_AUDIT_TEMPLATE, "root", "")).size());
    
  }
  
  @AfterClass
  public static void tearDownMiniCluster() throws Exception {
    accumulo.stop();
    
    // Comment this out to have a look at the logs, they will be in /tmp/junit*
    folder.delete();
  }
}
