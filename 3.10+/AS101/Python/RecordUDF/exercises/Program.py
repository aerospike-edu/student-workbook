#!/usr/bin/env python
# 
#  * Copyright 2012-2014 by Aerospike.
#  *
#  * Permission is hereby granted, free of charge, to any person obtaining a copy
#  * of this software and associated documentation files (the "Software"), to
#  * deal in the Software without restriction, including without limitation the
#  * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
#  * sell copies of the Software, and to permit persons to whom the Software is
#  * furnished to do so, subject to the following conditions:
#  *
#  * The above copyright notice and this permission notice shall be included in
#  * all copies or substantial portions of the Software.
#  *
#  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
#  * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
#  * IN THE SOFTWARE.
#  
from __future__ import print_function
import aerospike
import sys
from optparse import OptionParser
from UserService import UserService
from TweetService import TweetService

# 
#  * @author Raghavendra Kumar  
#  
class Program(object):
    client=None 
    seedHost = str()
    port = int()
    namespace = str()
    set = str()
    writePolicy = {}
    policy = {}

    def __init__(self, host, port, namespace, set):
        #  Establish a connection to Aerospike cluster
        self.client = aerospike.client({ 'hosts': [ (host, port) ] }).connect()
        self.seedHost = host
        self.port = port
        self.namespace = namespace
        self.set = set
        self.writePolicy = {}
        self.policy = {} 

    @classmethod
    def main(cls, args):
        usage = "usage: %prog [options] "
        optparser = OptionParser(usage=usage, add_help_option=False)
        optparser.add_option( "--help", dest="help", action="store_true", help="Displays this message.")
        optparser.add_option( "-h", "--host", dest="host", type="string", default="127.0.0.1", metavar="<ADDRESS>", help="Address of Aerospike server (default: 127.0.0.1)")
        optparser.add_option( "-p", "--port", dest="port", type="int", default=3000, metavar="<PORT>", help="Port of the Aerospike server (default: 3000)")
        optparser.add_option( "-n", "--namespace", dest="namespace", type="string", metavar="<NAMESPACE>", help="Namespace (default: test)")
        optparser.add_option( "-s", "--set", dest="set", type="string",metavar="<SET>", help="Set (default: demo)")
        (options, args) = optparser.parse_args()
        if options.help:
            optparser.print_help()
            print()
            sys.exit(1)
        aero=Program(options.host,options.port,options.namespace,options.set)
        aero.work()


    def work(self):
        print("***** Welcome to Aerospike Developer Training *****\n")
        print("INFO: Connecting to Aerospike cluster...")
            #  Establish connection to Aerospike server
        if not self.client:
            print("\nERROR: Connection to Aerospike cluster failed! Please check the server settings and try again!")
        else:
              print("\nINFO: Connection to Aerospike cluster succeeded!\n")
              #  Create instance of UserService
              us = UserService(self.client)
              #  Create instance of TweetService
              ts = TweetService(self.client)
              #  Present options
              print("\nWhat would you like to do:\n")
              print("1> Create A User And A Tweet\n")
              print("2> Read A User Record\n")
              print("3> Batch Read Tweets For A User\n")
              print("4> Scan All Tweets For All Users\n")
              print("5> Record UDF -- Update User Password\n")
              print("6> Query Tweets By Username And Users By Tweet Count Range\n")
              print("7> Stream UDF -- Aggregation Based on Tweet Count By Region\n")
              print("0> Exit\n")
              print("\nSelect 0-7 and hit enter:\n")
              try:
                feature=int(raw_input('Input:'))
              except ValueError:
                print("Input a valid feature number")
                sys.exit(0)
              if feature != 0:
                  if feature==1:
                      print("\n********** Your Selection: Create User And A Tweet **********\n")
                      us.createUser()
                      ts.createTweet()
                  elif feature==2:
                      print("\n********** Your Selection: Read A User Record **********\n")
                      us.getUser()
                  elif feature==3:
                      print("\n********** Your Selection: Batch Read Tweets For A User **********\n")
                      us.batchGetUserTweets()
                  elif feature==4:
                      print("\n********** Your Selection: Scan All Tweets For All Users **********\n")
                      ts.scanAllTweetsForAllUsers()
                  elif feature==5:
                      print("\n********** Your Selection: Record UDF -- Update User Password **********\n")
                      us.updatePasswordUsingUDF()
                  elif feature==6:
                      print("\n********** Your Selection: Query Tweets By Username And Users By Tweet Count Range **********\n")
                      ts.queryTweetsByUsername()
                      ts.queryUsersByTweetCount()
                  elif feature==7:
                      print("\n********** Your Selection: Stream UDF -- Aggregation Based on Tweet Count By Region **********\n")
                      us.aggregateUsersByTweetCountByRegion()
                  elif feature==12:
                      print("\n********** Create Users **********\n")
                      us.createUsers()
                  elif feature==23:
                      print("\n********** Create Tweets **********\n")
                      ts.createTweets()
                  else:
                    print ("Enter a Valid number from above menue !!")


    # 
    # 	 * example method calls
    # 	 
    def readPartial(self, userName):
        """ Python read specific bins """
        (key, metadata, record) = self.client.get(("test", "users", userName), ("username", "password", "gender", "region") )
        return record

    def readMeta(self, userName):
        """ not supported in Python Client """

    def write(self, username, password):
        """  Python read-modify-write """
        meta = None
        wr_policy = {
                AS_POLICY_W_GEN:  AS_POLICY_GEN_EQ
        }
        key = ("test", "users", username)
        self.client.put(key,{"username": username,"password": password},meta,wr_policy)

    def delete(self, username):
        """ Delete Record  """
        key = ("test", "users", username)
        self.client.remove(key)

    def exisis(self, username):
        """ Python key exists """
        key = ("test", "users", username)
        (key,itsHere) = self.client.exists(key)
        # itsHere should not be Null
        return itsHere

    def add(self, username):
        """ Add """
        key = ("test", "users", username)
        self.client.put(key, {"tweetcount":1})

    def touch(self, username):
        """ Not supported in Python Client """

    def append(self, username):
        """ Not supported in Python Client """

    def connectWithClientPolicy(self):
        """ Connect with Client configs """
        config = { 'hosts': [ ( '127.0.0.1', 3000 )
         ],
         'policies': { 'timeout': 1000 # milliseconds
         } }
        client = aerospike.client(config)

    def deleteBin(self, username):
        """ generated source for method deleteBin """
        key = ("test", "users", username)
        #  Set bin value to null to drop bin.
        self.client.put(key, {"interests": None} )

AS_POLICY_W_GEN        = "generation"
AS_POLICY_GEN_UNDEF    = 0 # Use default value
AS_POLICY_GEN_IGNORE   = 1 # Write a record, regardless of generation.
AS_POLICY_GEN_EQ       = 2 # Write a record, ONLY if generations are equal
AS_POLICY_GEN_GT       = 3 # Write a record, ONLY if local generation is
                                                                                                         # greater-than remote generation.
AS_POLICY_GEN_DUP      = 4 # Write a record creating a duplicate, ONLY if

if __name__ == '__main__':
    import sys
    Program.main(sys.argv)

