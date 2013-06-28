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
package org.apache.accumulo.server.util;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Stack;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.fate.curator.CuratorReaderWriter.NodeExistsPolicy;
import org.apache.accumulo.server.curator.CuratorReaderWriter;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.beust.jcommander.Parameter;

public class RestoreZookeeper {
  
  private static class Restore extends DefaultHandler {
    CuratorReaderWriter zk = null;
    Stack<String> cwd = new Stack<String>();
    boolean overwrite = false;
    
    Restore(CuratorReaderWriter zk, boolean overwrite) {
      this.zk = zk;
      this.overwrite = overwrite;
    }
    
    @Override
    public void startElement(String uri, String localName, String name, Attributes attributes) throws SAXException {
      if ("node".equals(name)) {
        String child = attributes.getValue("name");
        if (child == null)
          throw new RuntimeException("name attribute not set");
        String encoding = attributes.getValue("encoding");
        String value = attributes.getValue("value");
        if (value == null)
          value = "";
        String path = cwd.lastElement() + "/" + child;
        create(path, value, encoding);
        cwd.push(path);
      } else if ("dump".equals(name)) {
        String root = attributes.getValue("root");
        cwd.push(root);
        create(root, "", "utf-8");
      }
    }
    
    @Override
    public void endElement(String uri, String localName, String name) throws SAXException {
      cwd.pop();
    }
    
    private void create(String path, String value, String encoding) {
      byte[] data = value.getBytes();
      if ("base64".equals(encoding))
        data = Base64.decodeBase64(value.getBytes());
      try {
        try {
          zk.putPersistentData(path, data, overwrite ? NodeExistsPolicy.OVERWRITE : NodeExistsPolicy.FAIL);
        } catch (KeeperException e) {
          if (e.code().equals(KeeperException.Code.NODEEXISTS))
            throw new RuntimeException(path + " exists.  Remove it first.");
          throw e;
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  static class Opts extends Help {
    @Parameter(names={"-z", "--keepers"})
    String keepers = "localhost:2181";
    @Parameter(names="--overwrite")
    boolean overwrite = false;
    @Parameter(names="--file")
    String file;
  }
  
  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Logger.getRootLogger().setLevel(Level.WARN);
    Opts opts = new Opts();
    opts.parseArgs(RestoreZookeeper.class.getName(), args);
    
    InputStream in = System.in;
    if (opts.file != null) {
      in = new FileInputStream(opts.file);
    }
    
    SAXParserFactory factory = SAXParserFactory.newInstance();
    SAXParser parser = factory.newSAXParser();
    parser.parse(in, new Restore(CuratorReaderWriter.getInstance(), opts.overwrite));
    in.close();
  }
}
