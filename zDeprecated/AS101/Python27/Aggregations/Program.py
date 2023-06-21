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
#Exercise K1
from aerospike.exception import *
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
        host = "127.0.0.1"  
        
        #Exercise K1, Exercise R2, Exercise Q3 & Exercise A2
        #Override with your AWS IP Address
        #host = "54.237.175.53"  
        
        # Exercise R2 & Exercise A2
        #TODO: Instantiate config = ... with Host list and lua path configurations
        
        
        # Exercise K1
        try:
            self.client = aerospike.client(config).connect()
        except ClientError as e:
            print("ERROR: Connection to Aerospike cluster failed!")
            print("Please check the server settings and try again!")
            print("Error: {0} [{1}]".format(e.msg, e.code))
            sys.exit(1)
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
        
        if self.client:
              print("\nINFO: Connection to Aerospike cluster succeeded!\n")
              #  Create instance of UserService
              us = UserService(self.client)
              #  Create instance of TweetService
              ts = TweetService(self.client)
              #  Present options
              print("\nWhat would you like to do:")
              print("1> Create A User")
              print("2> Create A Tweet By A User")
              print("3> Read A User Record")
              print("4> Batch Read Tweets For A User")
              print("5> Scan All Tweets For All Users")
              print("6> Update User Password using CAS")
              print("7> Update User Password using Record UDF")
              print("8> Query Tweets By Username")
              print("9> Query Users By Tweet Count Range")
              print("10> Stream UDF -- Aggregation Based on Tweet Count By Region")
              print("11> Create A Test Set of Users")
              print("12> Create A Test Set of Tweets")
              print("0> Exit\n")
              print("\nSelect 0-12 and hit enter:\n")
              try:
                feature=int(raw_input('Input:'))
              except ValueError:
                print("Input a valid feature number")
                sys.exit(0)
              if feature != 0:
                  if feature==1:
                      print("\n********** Your Selection: Create A User **********\n")
                      us.createUser()
                  elif feature==2:
                      print("\n********** Your Selection: Create A Tweet By A User **********\n")                      
                      ts.createTweet()
                  elif feature==3:
                      print("\n********** Your Selection: Read A User Record **********\n")
                      us.getUser()
                  elif feature==4:
                      print("\n********** Your Selection: Batch Read Tweets For A User **********\n")
                      us.batchGetUserTweets()
                  elif feature==5:
                      print("\n********** Your Selection: Scan All Tweets For All Users **********\n")
                      ts.scanAllTweetsForAllUsers()
                  elif feature==6:
                      print("\n********** Your Selection: Update User Password using CAS **********\n")
                      us.updatePasswordUsingCAS()
                  elif feature==7:
                      print("\n********** Your Selection: Update User Password using Record UDF **********\n")
                      us.updatePasswordUsingUDF()
                  elif feature==8:
                      print("\n********** Your Selection: Query Tweets By Username **********\n")
                      ts.queryTweetsByUsername()
                  elif feature==9:
                      print("\n********** Your Selection: Query Users By Tweet Count Range **********\n")
                      ts.queryUsersByTweetCount()
                  elif feature==10:
                      print("\n********** Your Selection: Stream UDF -- Aggregation Based on Tweet Count By Region **********\n")
                      us.aggregateUsersByTweetCountByRegion()
                  elif feature==11:
                      print("\n********** Create A Test Set of Users **********\n")
                      us.createUsers()
                  elif feature==12:                     
                      print("\n********** Create A Test Set of Tweets **********\n")
                      ts.createTweets()
                  else:
                      print ("Enter a Valid number from above menu !!")
              #Exercise K1
              self.client.close()
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
        wr_policy = {'gen':aerospike.POLICY_GEN_EQ}                
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
        """ Touch """
        key = ("test", "users", username)
        self.client.touch(key)
        
    def append(self, username, binname, str2append):
        """ Append """
        key = ("test", "users", username)
        self.client.append(key, binname, str2append)
        
    def connectWithClientPolicy(self):
        """ Connect with Client configs """
        config = { 'hosts': [ ( '127.0.0.1', 3000 ) 
         ], 
         'policies': { 'timeout': 1000 # milliseconds 
         } }
        client = aerospike.client(config) 

    def deleteBin(self, username):
        key = ("test", "users", username)
        #  Set bin value to null to drop bin.
        self.client.put(key, {"interests": None} )

if __name__ == '__main__':
    import sys
    Program.main(sys.argv)

