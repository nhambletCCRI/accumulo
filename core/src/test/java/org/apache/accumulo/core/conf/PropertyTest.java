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
package org.apache.accumulo.core.conf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.Test;

/**
 * Test the Property class
 */
public class PropertyTest {
  @Test
  public void testProperties() {
    HashSet<String> validPrefixes = new HashSet<String>();
    for (Property prop : Property.values())
      if (prop.getType().equals(PropertyType.PREFIX))
        validPrefixes.add(prop.getKey());
    
    HashSet<String> propertyNames = new HashSet<String>();
    for (Property prop : Property.values()) {
      // make sure properties default values match their type
      assertTrue("Property " + prop + " has invalid default value " + prop.getDefaultValue() + " for type " + prop.getType(),
          prop.getType().isValidFormat(prop.getDefaultValue()));
      
      // make sure property has a description
      assertFalse("Description not set for " + prop, prop.getDescription() == null || prop.getDescription().isEmpty());
      
      // make sure property starts with valid prefix
      boolean containsValidPrefix = false;
      for (String pre : validPrefixes)
        if (prop.getKey().startsWith(pre)) {
          containsValidPrefix = true;
          break;
        }
      assertTrue("Invalid prefix on prop " + prop, containsValidPrefix);
      
      // make sure properties aren't duplicate
      assertFalse("Duplicate property name " + prop.getKey(), propertyNames.contains(prop.getKey()));
      propertyNames.add(prop.getKey());
      
    }
  }
  
  @Test
  public void testPorts() {
    HashSet<Integer> usedPorts = new HashSet<Integer>();
    for (Property prop : Property.values())
      if (prop.getType().equals(PropertyType.PORT)) {
        int port = Integer.parseInt(prop.getDefaultValue());
        assertFalse("Port already in use: " + port, usedPorts.contains(port));
        usedPorts.add(port);
        assertTrue("Port out of range of valid ports: " + port, port > 1023 && port < 65536);
      }
  }
  
  private void typeCheckValidFormat(PropertyType type, String... args) {
    for (String s : args)
      assertTrue(s + " should be valid", type.isValidFormat(s));
  }
  
  private void typeCheckInvalidFormat(PropertyType type, String... args) {
    for (String s : args)
      assertFalse(s + " should be invalid", type.isValidFormat(s));
  }
  
  @Test
  public void testTypes() {
    typeCheckValidFormat(PropertyType.TIMEDURATION, "600", "30s", "45m", "30000ms", "3d", "1h");
    typeCheckInvalidFormat(PropertyType.TIMEDURATION, "1w", "1h30m", "1s 200ms", "ms", "", "a");
    
    typeCheckValidFormat(PropertyType.MEMORY, "1024", "20B", "100K", "1500M", "2G");
    typeCheckInvalidFormat(PropertyType.MEMORY, "1M500K", "1M 2K", "1MB", "1.5G", "1,024K", "", "a");
    
    typeCheckValidFormat(PropertyType.HOSTLIST, "localhost", "server1,server2,server3", "server1:1111,server2:3333", "localhost:1111", "server2:1111",
        "www.server", "www.server:1111", "www.server.com", "www.server.com:111");
    typeCheckInvalidFormat(PropertyType.HOSTLIST, ":111", "local host");
    
    typeCheckValidFormat(PropertyType.ABSOLUTEPATH, "/foo", "/foo/c", "/");
    // in hadoop 2.0 Path only normalizes Windows paths properly when run on a Windows system
    // this makes the following checks fail
    if (System.getProperty("os.name").toLowerCase().contains("windows"))
      typeCheckValidFormat(PropertyType.ABSOLUTEPATH, "d:\\foo12", "c:\\foo\\g", "c:\\foo\\c", "c:\\");
    typeCheckInvalidFormat(PropertyType.ABSOLUTEPATH, "foo12", "foo/g", "foo\\c");
  }
  
  @Test
  public void testRawDefaultValues() {
    AccumuloConfiguration conf = AccumuloConfiguration.getDefaultConfiguration();
    assertEquals("${java.io.tmpdir}" + File.separator + "accumulo-vfs-cache-${user.name}", Property.VFS_CLASSLOADER_CACHE_DIR.getRawDefaultValue());
    assertEquals(new File(System.getProperty("java.io.tmpdir"), "accumulo-vfs-cache-" + System.getProperty("user.name")).getAbsolutePath(),
        conf.get(Property.VFS_CLASSLOADER_CACHE_DIR));
  }
  
  @Test
  public void testSensitiveKeys() {
    final TreeMap<String,String> extras = new TreeMap<String,String>();
    extras.put("trace.token.property.blah", "something");
    
    AccumuloConfiguration conf = new DefaultConfiguration() {
      @Override
      public Iterator<Entry<String,String>> iterator() {
        final Iterator<Entry<String,String>> parent = super.iterator();
        final Iterator<Entry<String,String>> mine = extras.entrySet().iterator();
        
        return new Iterator<Entry<String,String>>() {
          
          @Override
          public boolean hasNext() {
            return parent.hasNext() || mine.hasNext();
          }
          
          @Override
          public Entry<String,String> next() {
            return parent.hasNext() ? parent.next() : mine.next();
          }
          
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
    TreeSet<String> expected = new TreeSet<String>();
    for (Entry<String,String> entry : conf) {
      String key = entry.getKey();
      if (key.equals(Property.INSTANCE_SECRET.getKey()) || key.toLowerCase().contains("password") || key.toLowerCase().contains("secret")
          || key.startsWith(Property.TRACE_TOKEN_PROPERTY_PREFIX.getKey()))
        expected.add(key);
    }
    TreeSet<String> actual = new TreeSet<String>();
    for (Entry<String,String> entry : conf) {
      String key = entry.getKey();
      if (Property.isSensitive(key))
        actual.add(key);
    }
    assertEquals(expected, actual);
  }
}
