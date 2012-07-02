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
package org.apache.accumulo.core.util;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.LocalityGroupUtil.LocalityGroupConfigurationError;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class LocalityGroupUtilTest {
  
  @Test
  public void testColumnFamilySet() {
    
    SiteConfiguration conf = AccumuloConfiguration.getSiteConfiguration();
    conf.set("table.group.lg1", "cf1,cf2");
    conf.set("table.groups.enabled", "lg1");
    try {
      Map<String,Set<ByteSequence>> groups = LocalityGroupUtil.getLocalityGroups(conf);
      Assert.assertEquals(1, groups.size());
      Assert.assertNotNull(groups.get("lg1"));
      Assert.assertEquals(2, groups.get("lg1").size());
      Assert.assertTrue(groups.get("lg1").contains(new ArrayByteSequence("cf1")));
    } catch (LocalityGroupConfigurationError err) {
      Assert.fail();
    }
    try {
      conf.set("table.group.lg2", "cf1");
      conf.set("table.groups.enabled", "lg1,lg2");
      LocalityGroupUtil.getLocalityGroups(conf);
      Assert.fail();
    } catch (LocalityGroupConfigurationError err) {}
  }
  
  @Test
  public void testEncoding() throws Exception {
    byte test1[] = new byte[256];
    byte test2[] = new byte[256];
    for (int i = 0; i < 256; i++) {
      test1[i] = (byte) (0xff & i);
      test2[i] = (byte) (0xff & (255 - i));
    }
    
    ArrayByteSequence bs1 = new ArrayByteSequence(test1);
    
    String ecf = LocalityGroupUtil.encodeColumnFamily(bs1);
    
    // System.out.println(ecf);
    
    ByteSequence bs2 = LocalityGroupUtil.decodeColumnFamily(ecf);
    
    Assert.assertEquals(bs1, bs2);
    Assert.assertEquals(ecf, LocalityGroupUtil.encodeColumnFamily(bs2));
    
    // test encoding multiple column fams containing binary data
    HashSet<Text> in = new HashSet<Text>();
    HashSet<ByteSequence> in2 = new HashSet<ByteSequence>();
    in.add(new Text(test1));
    in2.add(new ArrayByteSequence(test1));
    in.add(new Text(test2));
    in2.add(new ArrayByteSequence(test2));
    Set<ByteSequence> out = LocalityGroupUtil.decodeColumnFamilies(LocalityGroupUtil.encodeColumnFamilies(in));
    
    Assert.assertEquals(in2, out);
  }
  
}