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
package org.apache.accumulo.start;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Set;

import org.apache.accumulo.start.classloader.AccumuloClassLoader;

public class Main {
  
  public static void main(String[] args) throws Exception {
    Runnable r = null;
    
    try {
      Thread.currentThread().setContextClassLoader(AccumuloClassLoader.getClassLoader());
      Class<?> vfsClassLoader = AccumuloClassLoader.getClassLoader().loadClass("org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader");
      ClassLoader cl = (ClassLoader) vfsClassLoader.getMethod("getClassLoader", new Class[] {}).invoke(null, new Object[] {});
      Thread.currentThread().setContextClassLoader(cl);
      
      if (args.length == 0) {
        printUsage();
        System.exit(1);
      }
      final String argsToPass[] = new String[args.length - 1];
      System.arraycopy(args, 1, argsToPass, 0, args.length - 1);
      
      Class<?> runTMP = null;
      
      if (args[0].equals("classpath")) {
        vfsClassLoader.getMethod("printClassPath", new Class[] {}).invoke(vfsClassLoader, new Object[] {});
        return;
      } else if (args[0].equals("version")) {
        runTMP = cl.loadClass("org.apache.accumulo.core.Constants");
        System.out.println(runTMP.getField("VERSION").get(null));
        return;
      } else {
        runTMP = getAccumuloServiceClassByKeyword(args[0]);
        if (runTMP == null) {
          try {
            runTMP = cl.loadClass(args[0]);
          } catch (ClassNotFoundException cnfe) {
            System.out.println("Classname " + args[0] + " not found.  Please make sure you use the wholly qualified package name.");
            System.exit(1);
          }
        }
      }
      Method main = null;
      try {
        main = runTMP.getMethod("main", args.getClass());
      } catch (Throwable t) {
        t.printStackTrace();
      }
      if (main == null || !Modifier.isPublic(main.getModifiers()) || !Modifier.isStatic(main.getModifiers())) {
        System.out.println(args[0] + " must implement a public static void main(String args[]) method");
        System.exit(1);
      }
      final Object thisIsJustOneArgument = argsToPass;
      final Method finalMain = main;
      r = new Runnable() {
        @Override
        public void run() {
          try {
            finalMain.invoke(null, thisIsJustOneArgument);
          } catch (Exception e) {
            System.err.println("Thread \"" + Thread.currentThread().getName() + "\" died " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
          }
        }
      };
      
      Thread t = new Thread(r, args[0]);
      t.setContextClassLoader(cl);
      t.start();
    } catch (Throwable t) {
      System.err.println("Uncaught exception: " + t.getMessage());
      t.printStackTrace(System.err);
    }
  }
  
  private static void printUsage() throws Exception {
    ArrayList<String> keywords = new ArrayList<String>(20);
    for (String keyword : getAccumuloServiceKeywords()) {
      keywords.add(keyword);
    }
    keywords.add("classpath");
    keywords.add("version");
    
    String prefix = "";
    String kwString = "";
    for (String kw : keywords) {
      kwString += prefix + kw;
      prefix = " | ";
    }
    System.out.println("accumulo " + kwString + " | <accumulo class> args");
  }
  
  private static Set<String> getAccumuloServiceKeywords() throws Exception {
    Class<?> vfsClassLoader = AccumuloClassLoader.getClassLoader().loadClass("org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader");
    @SuppressWarnings("unchecked")
    Set<String> keywords = (Set<String>) vfsClassLoader.getMethod("getAccumuloServiceKeywords").invoke(null);
    return keywords;
  }
  
  private static Class<?> getAccumuloServiceClassByKeyword(String keyword) throws Exception {
    Class<?> vfsClassLoader = AccumuloClassLoader.getClassLoader().loadClass("org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader");
    Class<?> serviceClass = (Class<?>) vfsClassLoader.getMethod("getAccumuloServiceClassByKeyword", String.class).invoke(null, keyword);
    return serviceClass;
  }
}
