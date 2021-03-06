#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/../../../../conf/accumulo-env.sh

if [ -z $HADOOP_HOME ] ; then
    echo "HADOOP_HOME is not set.  Please make sure it's set globally."
    exit 1
fi

if [ -z $ACCUMULO_HOME ] ; then
    echo "ACCUMULO_HOME is not set.  Please make sure it's set globally."
    exit 1
fi

if [ "$1" = "" ] ; then
     echo "Usage: start-local.sh <startNode>"
     exit 1
fi

RW_HOME=$ACCUMULO_HOME/test/system/randomwalk

cd $RW_HOME

# grab config from HDFS
$HADOOP_HOME/bin/hadoop fs -get /randomwalk/config.tgz config.tgz

# extract config to a tmp directory
rm -rf tmp/
mkdir tmp/
tar xzf config.tgz -C tmp/
rm config.tgz

# config the logging
RW_LOGS=$RW_HOME/logs
LOG_ID=`hostname -s`_`date +%Y%m%d_%H%M%S`

# run the local walker
$ACCUMULO_HOME/bin/accumulo org.apache.accumulo.server.test.randomwalk.Framework $RW_HOME/tmp/conf/ $RW_LOGS/$LOG_ID $1 >$RW_LOGS/$LOG_ID.out 2>$RW_LOGS/$LOG_ID.err &
