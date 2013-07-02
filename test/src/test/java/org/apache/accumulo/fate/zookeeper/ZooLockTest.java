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
package org.apache.accumulo.fate.zookeeper;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.fate.curator.CuratorReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooLock.AsyncLockWatcher;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockLossReason;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ZooLockTest {
  
  public static TemporaryFolder folder = new TemporaryFolder();
  
  private static MiniAccumuloCluster accumulo;
  
  static class TestALW implements AsyncLockWatcher {
    
    LockLossReason reason = null;
    boolean locked = false;
    Exception exception = null;
    int changes = 0;
    
    @Override
    public synchronized void lostLock(LockLossReason reason) {
      this.reason = reason;
      changes++;
      this.notifyAll();
    }
    
    @Override
    public synchronized void acquiredLock() {
      this.locked = true;
      changes++;
      this.notifyAll();
    }
    
    @Override
    public synchronized void failedToAcquireLock(Exception e) {
      this.exception = e;
      changes++;
      this.notifyAll();
    }
    
    public synchronized void waitForChanges(int numExpected) throws InterruptedException {
      while (changes < numExpected) {
        this.wait();
      }
    }
    
    @Override
    public synchronized void unableToMonitorLockNode(Throwable e) {
      changes++;
      this.notifyAll();
    }
  }
  
  @BeforeClass
  public static void setupMiniCluster() throws Exception {
    
    folder.create();
    
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);
    
    accumulo = new MiniAccumuloCluster(folder.getRoot(), "superSecret");
    
    accumulo.start();
    
  }
  
  private static int pdCount = 0;
  
  @Deprecated
  @Test(timeout = 10000)
  public void testDeleteParent() throws Exception {
    accumulo.getConfig().getZooKeepers();
    
    String parent = "/zltest-" + this.hashCode() + "-l" + pdCount++;
    
    ZooLock zl = new ZooLock(accumulo.getConfig().getZooKeepers(), 30000, "digest", "secret".getBytes(), parent);
    
    Assert.assertFalse(zl.isLocked());
    
    CuratorReaderWriter zk = new CuratorReaderWriter(accumulo.getConfig().getZooKeepers(), 30000, "digest", "secret".getBytes());
    
    // intentionally created parent after lock
    zk.mkdirs(parent);
    
    zk.delete(parent, -1);
    
    zk.mkdirs(parent);
    
    TestALW lw = new TestALW();
    
    zl.lockAsync(lw, "test1".getBytes());
    
    lw.waitForChanges(1);
    
    Assert.assertTrue(lw.locked);
    Assert.assertTrue(zl.isLocked());
    Assert.assertNull(lw.exception);
    Assert.assertNull(lw.reason);
    
    zl.unlock();
  }
  
  @Test(timeout = 10000)
  public void testNoParent() throws Exception {
    accumulo.getConfig().getZooKeepers();
    
    String parent = "/zltest-" + this.hashCode() + "-l" + pdCount++;
    
    ZooLock zl = new ZooLock(accumulo.getConfig().getZooKeepers(), 30000, "digest", "secret".getBytes(), parent);
    
    Assert.assertFalse(zl.isLocked());
    
    TestALW lw = new TestALW();
    
    zl.lockAsync(lw, "test1".getBytes());
    
    lw.waitForChanges(1);
    
    Assert.assertFalse(lw.locked);
    Assert.assertFalse(zl.isLocked());
    Assert.assertNotNull(lw.exception);
    Assert.assertNull(lw.reason);
  }
  
  @Deprecated
  @Test(timeout = 10000)
  public void testDeleteLock() throws Exception {
    accumulo.getConfig().getZooKeepers();
    
    String parent = "/zltest-" + this.hashCode() + "-l" + pdCount++;
    
    CuratorReaderWriter zk = new CuratorReaderWriter(accumulo.getConfig().getZooKeepers(), 30000, "digest", "secret".getBytes());
    zk.mkdirs(parent);
    
    ZooLock zl = new ZooLock(accumulo.getConfig().getZooKeepers(), 30000, "digest", "secret".getBytes(), parent);
    
    Assert.assertFalse(zl.isLocked());
    
    TestALW lw = new TestALW();
    
    zl.lockAsync(lw, "test1".getBytes());
    
    lw.waitForChanges(1);
    
    Assert.assertTrue(lw.locked);
    Assert.assertTrue(zl.isLocked());
    Assert.assertNull(lw.exception);
    Assert.assertNull(lw.reason);
    
    zk.delete(zl.getLockPath(), -1);
    
    lw.waitForChanges(2);
    
    Assert.assertEquals(LockLossReason.LOCK_DELETED, lw.reason);
    Assert.assertNull(lw.exception);
    
  }
  
  @Test(timeout = 10000)
  public void testDeleteWaiting() throws Exception {
    accumulo.getConfig().getZooKeepers();
    
    String parent = "/zltest-" + this.hashCode() + "-l" + pdCount++;
    
    // TODO
    CuratorReaderWriter zk = new CuratorReaderWriter(accumulo.getConfig().getZooKeepers(), 30000, "digest", "secret".getBytes());
    zk.mkdirs(parent);
    
    ZooLock zl = new ZooLock(accumulo.getConfig().getZooKeepers(), 30000, "digest", "secret".getBytes(), parent);
    
    Assert.assertFalse(zl.isLocked());
    
    TestALW lw = new TestALW();
    
    zl.lockAsync(lw, "test1".getBytes());
    
    lw.waitForChanges(1);
    
    Assert.assertTrue(lw.locked);
    Assert.assertTrue(zl.isLocked());
    Assert.assertNull(lw.exception);
    Assert.assertNull(lw.reason);
    
    ZooLock zl2 = new ZooLock(accumulo.getConfig().getZooKeepers(), 30000, "digest", "secret".getBytes(), parent);
    
    TestALW lw2 = new TestALW();
    
    zl2.lockAsync(lw2, "test2".getBytes());
    
    Assert.assertFalse(lw2.locked);
    Assert.assertFalse(zl2.isLocked());
    
    ZooLock zl3 = new ZooLock(accumulo.getConfig().getZooKeepers(), 30000, "digest", "secret".getBytes(), parent);
    
    TestALW lw3 = new TestALW();
    
    zl3.lockAsync(lw3, "test3".getBytes());
    
    List<String> children = zk.getChildren(parent);
    Collections.sort(children);
    
    zk.delete(parent + "/" + children.get(1), -1);
    
    lw2.waitForChanges(1);
    
    Assert.assertFalse(lw2.locked);
    Assert.assertNotNull(lw2.exception);
    Assert.assertNull(lw2.reason);
    
    zk.delete(parent + "/" + children.get(0), -1);
    
    lw.waitForChanges(2);
    
    Assert.assertEquals(LockLossReason.LOCK_DELETED, lw.reason);
    Assert.assertNull(lw.exception);
    
    lw3.waitForChanges(1);
    
    Assert.assertTrue(lw3.locked);
    Assert.assertTrue(zl3.isLocked());
    Assert.assertNull(lw3.exception);
    Assert.assertNull(lw3.reason);
    
    zl3.unlock();
    
  }
  
  @Test(timeout = 10000)
  public void testUnexpectedEvent() throws Exception {
    accumulo.getConfig().getZooKeepers();
    
    String parent = "/zltest-" + this.hashCode() + "-l" + pdCount++;
    
    ZooKeeper zk = new ZooKeeper(accumulo.getConfig().getZooKeepers(), 30000, null);
    zk.addAuthInfo("digest", "secret".getBytes());
    
    zk.create(parent, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    
    ZooLock zl = new ZooLock(accumulo.getConfig().getZooKeepers(), 30000, "digest", "secret".getBytes(), parent);
    
    Assert.assertFalse(zl.isLocked());
    
    // would not expect data to be set on this node, but it should not cause problems.....
    zk.setData(parent, "foo".getBytes(), -1);
    
    TestALW lw = new TestALW();
    
    zl.lockAsync(lw, "test1".getBytes());
    
    lw.waitForChanges(1);
    
    Assert.assertTrue(lw.locked);
    Assert.assertTrue(zl.isLocked());
    Assert.assertNull(lw.exception);
    Assert.assertNull(lw.reason);
    
    // would not expect data to be set on this node either
    zk.setData(zl.getLockPath(), "bar".getBytes(), -1);
    
    zk.delete(zl.getLockPath(), -1);
    
    lw.waitForChanges(2);
    
    Assert.assertEquals(LockLossReason.LOCK_DELETED, lw.reason);
    Assert.assertNull(lw.exception);
    
  }
  
  @Test(timeout = 10000)
  public void testTryLock() throws Exception {
    String parent = "/zltest-" + this.hashCode() + "-l" + pdCount++;
    
    ZooLock zl = new ZooLock(accumulo.getConfig().getZooKeepers(), 1000, "digest", "secret".getBytes(), parent);
    
    ZooKeeper zk = new ZooKeeper(accumulo.getConfig().getZooKeepers(), 1000, null);
    zk.addAuthInfo("digest", "secret".getBytes());
    
    for (int i = 0; i < 10; i++) {
      zk.create(parent, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      zk.delete(parent, -1);
    }
    
    zk.create(parent, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    
    TestALW lw = new TestALW();
    
    boolean ret = zl.tryLock(lw, "test1".getBytes());
    
    Assert.assertTrue(ret);
    
    // make sure still watching parent even though a lot of events occurred for the parent
    synchronized (zl) {
      Field field = zl.getClass().getDeclaredField("watchingParent");
      field.setAccessible(true);
      Assert.assertTrue((Boolean) field.get(zl));
    }
    
    zl.unlock();
  }
  
  @AfterClass
  public static void tearDownMiniCluster() throws Exception {
    accumulo.stop();
    folder.delete();
  }
  
}
