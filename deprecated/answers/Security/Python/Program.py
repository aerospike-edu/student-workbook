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
from RoleService import RoleService

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

    def __init__(self, username, password, host, port, namespace, set):
        #  Establish a connection to Aerospike cluster
        print(username)
        print(password)
        self.client = aerospike.client({ 'hosts': [ (host, port) ]  }).connect(username, password)
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

        optparser.add_option( "-U", "--username", dest="username", type="string", default="superman", metavar="<USERNAME>", help="username")
        optparser.add_option( "-P", "--password", dest="password", type="string", default="krypton", metavar="<PASSWORD>", help="password")

        optparser.add_option( "-h", "--host", dest="host", type="string", default="127.0.0.1", metavar="<ADDRESS>", help="Address of Aerospike server (default: 127.0.0.1)")
        optparser.add_option( "-p", "--port", dest="port", type="int", default=3000, metavar="<PORT>", help="Port of the Aerospike server (default: 3000)")
        optparser.add_option( "-n", "--namespace", dest="namespace", type="string", metavar="<NAMESPACE>", help="Namespace (default: test)")
        optparser.add_option( "-s", "--set", dest="set", type="string",metavar="<SET>", help="Set (default: demo)")
        (options, args) = optparser.parse_args()
        if options.help:
            optparser.print_help()
            print()
            sys.exit(1)
        aero=Program(options.username, options.password, options.host,options.port,options.namespace,options.set)
        aero.work()


    def work(self):
        print("***** Welcome to Aerospike Developer Training *****\n")
        print("INFO: Connecting to Aerospike cluster...")
            #  Establish connection to Aerospike server
        if not self.client:
            print("\nERROR: Connection to Aerospike cluster failed! Please check the server settings and try again!")
            self.readLine()
        else:
              print("\nINFO: Connection to Aerospike cluster succeeded!\n")
              #  Create instance of UserService
              us = UserService(self.client)
              #  Create instance of RoleService
              rs = RoleService(self.client)
              #  Present options
              print("\nWhat would you like to do:\n")
              print("1> Create User\n")
              print("2> Read User\n")
              print("3> Grant Role to User\n")
              print("4> Revoke Role from User\n")
              print("5> Drop User\n")
              print("6> Create Role\n")
              print("7> Read Role\n")
              print("8> Grant Privilege to Role\n")
              print("9> Revoke Privilege from Role\n")
              print("10> Drop Role\n")
              print("0> Exit\n")
              print("\nSelect 0-10 and hit enter:\n")

              try:
                feature=int(raw_input('Input:'))
              except ValueError:
                print("Input a valid feature number")
                sys.exit(0)
              if feature != 0:
                  if feature==1:
                      print("\n********** Your Selection: Create User **********\n")
                      us.create_user()
                  elif feature==2:
                      print("\n********** Your Selection: Read User **********\n")
                      us.get_user()
                  elif feature==3:
                      print("\n********** Your Selection: Grant Role to User **********\n")
                      us.grant_role()
                  elif feature==4:
                      print("\n********** Your Selection: Revoke Role from User **********\n")
                      us.revoke_role()
                  elif feature==5:
                      print("\n********** Your Selection: Drop User **********\n")
                      us.drop_user()
                  elif feature==6:
                      print("\n********** Your Selection: Create Role **********\n")
                      rs.create_role()
                  elif feature==7:
                      print("\n********** Your Selection: Read Role **********\n")
                      rs.read_role()
                  elif feature==8:
                      print("\n********** Your Selection: Grant Privilege to Role **********\n")
                      rs.grant_privilege()
                  elif feature==9:
                      print("\n********** Your Selection: Revoke Privilege from Role **********\n")
                      rs.revoke_privilege()
                  elif feature==10:
                      print("\n********** Your Selection: Drop Role **********\n")
                      rs.drop_role()
                  else:
                    print ("Enter a Valid number from above menue !!")

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
