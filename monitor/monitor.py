#!/usr/bin/python
# -*- coding: utf-8 -*-
##########################################################################
# Copyright 2013-2016 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##########################################################################


from __future__ import print_function

import aerospike
import sys
import smtplib  #for sending email
from optparse import OptionParser

##########################################################################
# Options Parsing
##########################################################################

usage = "usage: %prog [options] [REQUEST]"

optparser = OptionParser(usage=usage, add_help_option=False)

optparser.add_option(
    "--help", dest="help", action="store_true",
    help="Displays this message.")

optparser.add_option(
    "-U", "--username", dest="username", type="string", metavar="<USERNAME>",
    help="Username to connect to database.")

optparser.add_option(
    "-P", "--password", dest="password", type="string", metavar="<PASSWORD>",
    help="Password to connect to database.")

optparser.add_option(
    "-h", "--host", dest="host", type="string", default="52.66.212.176", metavar="<ADDRESS>",
    help="Address of Aerospike server.")

optparser.add_option(
    "-p", "--port", dest="port", type="int", default=3000, metavar="<PORT>",
    help="Port of the Aerospike server.")

(options, args) = optparser.parse_args()

if options.help:
    optparser.print_help()
    print()
    sys.exit(1)

##########################################################################
# Client Configuration
##########################################################################

config = {
    'hosts': [(options.host, options.port)]
}

##########################################################################
# Application
##########################################################################
emsg = "Aerospike Server Report \n"
exitCode = 0
try:

    # ----------------------------------------------------------------------------
    # Connect to Cluster
    # ----------------------------------------------------------------------------

    client = aerospike.client(config).connect(
        options.username, options.password)

    # ----------------------------------------------------------------------------
    # Perform Operation
    # ----------------------------------------------------------------------------

    try:
        # -------------------------------------------------
        # Gather "namespace" for metrics for namespaces
        # First find all declared namespaces
        # Then, loop through them.
        # -------------------------------------------------
        request = "namespaces"  # request for getting all namespace names 
        if len(args) > 0:
            request = ' '.join(args)
        #print ("request= ", request) 
        nslist = []
        for node, (err, res) in list(client.info(request).items()):
            if res is not None:
                res = res.strip()
                if len(res) > 0:
                    nslist = res.split(';')
            break # assume all namespaces present on first node
        #print("Namespaces: "+' '.join(nslist))

        #---------- LOOP THRU COLLECTED DECLARED NAMESPACES ----------------
       
        for ns in nslist: 
            emsg = emsg + "Namespace: {0}:\n".format(ns)
            request = "namespace/"+ns  # request for namespace ns1  (repeat / loop for multiple namespaces)
            if len(args) > 0:
                request = ' '.join(args)
            #print ("request= ", request) 

            # --- Use filter below to select the metrics of interest ---

            filter = ["master_objects", 
                      "device_free_pct", 
                      "device_available_pct", 
                      "memory_free_pct",
                      "evicted_objects",
                      "clock_skew_stop_writes", 
                      "stop_writes", 
                      "hwm_breached",
                      "dead_partitions",
                      "unavailable_partitions"
                     ]

            # --- Loop thru all nodes of the cluster ---

            for node, (err, res) in list(client.info(request).items()):
                if res is not None:
                    res = res.strip()
                    if len(res) > 0:
                        entries = res.split(';')
                        if len(entries) > 1:
                            #print("NodeID {0}:".format(node))
                            emsg = emsg + "NodeID {0}:\n".format(node)
                            for entry in entries:
                                entry = entry.strip()
                                if len(entry) > 0:
                                    count = 0
                                    if "=" in entry:
                                        (name, value) = entry.split('=')
                                        if name in filter:
                                          if count > 0:
                                            #print(
                                            #    "      {0}: {1}".format(name, value))
                                            emsg = emsg +  "      {0}: {1}\n".format(name, value)
                                          else:
                                            #print(
                                            #    "    - {0}: {1}".format(name, value))
                                            emsg = emsg +  "     - {0}: {1}\n".format(name, value)
                                        count += 1
                        else:
                            #print("{0}: {1}".format(node, res))
                            emsg = emsg + "{0}: {1}\n".format(node, res)

        # ---------------------------------------
        # Gather "statistics" for Cluster metrics
        # ---------------------------------------

        request = "statistics"  # request for namespace ns1  (repeat / loop for multiple namespaces)
        if len(args) > 0:
            request = ' '.join(args)
        #print ("request= ", request) 
        filter = ["cluster_size"] 
        for node, (err, res) in list(client.info(request).items()):
            if res is not None:
                res = res.strip()
                if len(res) > 0:
                    entries = res.split(';')
                    if len(entries) > 1:
                        #print("NodeID {0}:".format(node))
                        emsg = emsg + "Cluster:\n"
                        for entry in entries:
                            entry = entry.strip()
                            if len(entry) > 0:
                                count = 0
                                if "=" in entry:
                                    (name, value) = entry.split('=')
                                    if name in filter:
                                      if count > 0:
                                        #print(
                                        #    "      {0}: {1}".format(name, value))
                                        emsg = emsg +  "      {0}: {1}\n".format(name, value)
                                      else:
                                        #print(
                                        #    "    - {0}: {1}".format(name, value))
                                        emsg = emsg +  "     - {0}: {1}\n".format(name, value)
                                    count += 1
                    else:
                        #print("{0}: {1}".format(node, res))
                        emsg = emsg + "{0}: {1}\n".format(node, res)
            break  #Don't loop through all nodes for cluster metrics
    except Exception as e:
        print("error: {0}".format(e), file=sys.stderr)
        exitCode = 2

    # ----------------------------------------------------------------------------
    # Close Connection to Cluster
    # ----------------------------------------------------------------------------

    client.close()

    # ----------------------------------------------------------------------------
    # Send email notifications
    # ----------------------------------------------------------------------------

    server = smtplib.SMTP('smtp.gmail.com',587)
    server.ehlo()
    server.starttls()
    server.login("yourmonitor@gmail.com", "emailPassword")

    from email.MIMEMultipart import MIMEMultipart
    from email.MIMEText import MIMEText
    fromaddr = "yourmonitor@gmail.com"
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = "user1@yourcompany.com"
    #msg['To'] = "user1@yourcompany.com,9255551212@txt.att.net"
    msg['Cc'] = " "
    #msg['Cc'] = "user2@gmail.com,user3@yourcompany.com"
    msg['Subject'] = "Aerospike Cluster Report -Testing"
    msg.attach(MIMEText(emsg, 'plain'))
    content = msg.as_string()

    # --- For testing, uncomment print, comment sendmail. ---

    #print("emailing: "+emsg)
    server.sendmail(fromaddr, msg['To'].split(",")+msg['Cc'].split(","), content)

    # ----------------------------------------------------------------------------


except Exception as eargs:
    print("error: {0}".format(eargs), file=sys.stderr)
    exitCode = 3

##########################################################################
# Exit
##########################################################################

sys.exit(exitCode)
