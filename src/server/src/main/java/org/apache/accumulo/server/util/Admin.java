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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;

import jline.ConsoleReader;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.impl.HdfsZooInstance;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

public class Admin {
  private static final Logger log = Logger.getLogger(Admin.class);
  
  public static void main(String[] args) {
    boolean everything;
    
    CommandLine cl = null;
    Options opts = new Options();
    opts.addOption("u", true, "optional administrator user name");
    opts.addOption("p", true, "optional administrator password");
    opts.addOption("?", "help", false, "displays the help");
    String user = null;
    byte[] pass = null;
    
    try {
      cl = new BasicParser().parse(opts, args);
      if (cl.hasOption("?"))
        throw new ParseException("help requested");
      args = cl.getArgs();
      
      user = cl.hasOption("u") ? cl.getOptionValue("u") : "root";
      pass = cl.hasOption("p") ? cl.getOptionValue("p").getBytes() : null;
      
      if (!((cl.getArgs().length == 1 && (args[0].equalsIgnoreCase("stopMaster") || args[0].equalsIgnoreCase("stopAll"))) || (cl.getArgs().length == 2 && args[0]
          .equalsIgnoreCase("stop"))))
        throw new ParseException("Incorrect arguments");
      
    } catch (ParseException e) {
      // print to the log and to stderr
      if (cl == null || !cl.hasOption("?"))
        log.error(e, e);
      HelpFormatter h = new HelpFormatter();
      StringWriter str = new StringWriter();
      h.printHelp(new PrintWriter(str), h.getWidth(), Admin.class.getName() + " stopMaster | stopAll | stop <tserver>", null, opts, h.getLeftPadding(),
          h.getDescPadding(), null, true);
      if (cl != null && cl.hasOption("?"))
        log.info(str.toString());
      else
        log.error(str.toString());
      h.printHelp(new PrintWriter(System.err), h.getWidth(), Admin.class.getName() + " stopMaster | stopAll | stop <tserver>", null, opts, h.getLeftPadding(),
          h.getDescPadding(), null, true);
      System.exit(3);
    }
    
    try {
      AuthInfo creds;
      if (args[0].equalsIgnoreCase("stop")) {
        stopTabletServer(args[1]);
      } else {
        if (!cl.hasOption("u") && !cl.hasOption("p")) {
          creds = SecurityConstants.systemCredentials;
        } else {
          if (pass == null) {
            try {
              pass = new ConsoleReader().readLine("Enter current password for '" + user + "': ", '*').getBytes();
            } catch (IOException ioe) {
              log.error("Password not specified and unable to prompt: " + ioe);
              System.exit(4);
            }
          }
          creds = new AuthInfo(user, pass, HdfsZooInstance.getInstance().getInstanceID());
        }
        
        everything = args[0].equalsIgnoreCase("stopAll");
        stopServer(creds, everything);
      }
    } catch (AccumuloException e) {
      log.error(e);
      System.exit(1);
    } catch (AccumuloSecurityException e) {
      log.error(e);
      System.exit(2);
    }
  }
  
  private static void stopServer(AuthInfo credentials, boolean tabletServersToo) throws AccumuloException, AccumuloSecurityException {
    MasterClientService.Iface client = connect();
    try {
      client.shutdown(null, credentials, tabletServersToo);
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (TException e) {
      throw new AccumuloException(e);
    } finally {
      MasterClient.close(client);
    }
  }
  
  private static void stopTabletServer(String server) throws AccumuloException, AccumuloSecurityException {
    MasterClientService.Iface client = connect();
    InetSocketAddress address = AddressUtil.parseAddress(server, Property.TSERV_CLIENTPORT);
    server = AddressUtil.toString(address);
    try {
      client.shutdownTabletServer(null, SecurityConstants.systemCredentials, server);
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (TException e) {
      throw new AccumuloException(e);
    } finally {
      MasterClient.close(client);
    }
  }
  
  private static MasterClientService.Iface connect() throws AccumuloException {
    MasterClientService.Iface client = null;
    try {
      client = MasterClient.getConnection(HdfsZooInstance.getInstance());
    } catch (Throwable t) {
      if (client != null)
        MasterClient.close(client);
      throw new AccumuloException(t.toString());
    }
    return client;
  }
}
