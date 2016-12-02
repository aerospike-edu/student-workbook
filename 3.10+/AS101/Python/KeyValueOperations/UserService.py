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
from aerospike.exception import *
import sys
import random
import time
from aerospike import predicates as p

class UserService(object):
    #client 

    def __init__(self, client):
        self.client = client

    def createUser(self):
        print("\n********** Create User **********\n")
        # /*********************///
        # /*****Data Model*****///
        # Namespace: test
        # Set: users
        # Key: <username>
        # Bins:
        # username - String
        # password - String (For simplicity password is stored in plain-text)
        # gender - String (Valid values are 'm' or 'f')
        # region - String (Valid values are: 'n' (North), 's' (South), 'e' (East), 'w' (West) -- to keep data entry to minimal we just store the first letter)
        # lasttweeted - int (Stores epoch timestamp of the last/most recent tweet) -- Default to 0
        # tweetcount - int (Stores total number of tweets for the user) -- Default to 0
        # interests - Array of interests
        # Sample Key: dash
        # Sample Record:
        # { username: 'dash',
        #   password: 'dash',
        #   gender: 'm',
        #   region: 'w',
        #   lasttweeted: 1408574221,
        #   tweetcount: 20,
        #   interests: ['photography', 'technology', 'dancing', 'house music] 
        # }
        # /*********************///
        username = str()
        password = str()
        gender = str()
        region = str()
        interests = str()
        # Exercise K2
        # TODO: Create Key or Bin(s) for the user record. 
        # Remember to convert comma separated interests to a List.
        #  Get username
        username = raw_input("Enter username: ") 
        #TODO: Create a record dictionary and assign 'username' key, username value 
        if len(username) > 0:
            #  Get password
            #TODO: Assign to password bin of the record: = raw_input("Enter password for " + username + ":") 
            #  Get gender
            #TODO: Assign to gender bin of the record: = raw_input("Select gender (f or m) for " + username + ":") 
            #  Get region
            #TODO: Assign to region bin of the record: = raw_input("Select region (north, south, east or west) for " + username + ":") 
            #  Get interests
            #TODO: Assign to interests bin of the record: = raw_input("Enter comma-separated interests for " + username + ":").split(',')
            #Initialize tweetcount
            #TODO: Assign to tweetcount bin of the record: = 0
            #  Write record            
            meta = None
            policy = None
            #TODO: Explicitly specify 'exists' policy to IGNORE
            
            #TODO: Using username as the Primary Key, write the record to Aerospike.
            
            print(record, "\nINFO: User record created!")

    def getUser(self):
        userRecord = None
        userKey = None
        #  Get username
        username = str()
        username = raw_input("Enter username: ")
        if len(username) > 0:
            #  Check if username exists
            # Exercise K2
            meta = None
            policy = None
            #TODO: Create userKey tuple
            
            #TODO: Read entire user record for userKey into a Tuple (key, metaData, userRecord)
            
            if userRecord:
                print("\nINFO: User record read successfully! Here are the details:\n")
                # Exercise K2
                #TODO: Output user record data to console.
                #TODO: print("username:   " , userRecord["username"] , "\n")
                #TODO: print password
                #TODO: print gender
                #TODO: print region
                #TODO: print tweetcount
                #TODD: print interests                
            else:
                print("ERROR: User record not found!\n")
        else:
            print("ERROR: User record not found!\n")

    def updatePasswordUsingUDF(self):
        userRecord = None
        userKey = None
        policy = {}
        udf_type = 0 # 0 for LUA
        lua_file_name = 'udf/updateUserPwd.lua'
        #  Get username
        username = str()
        username = raw_input("Enter username: ")
        if len(username) > 0:
            #  Check if username exists
            # Read user record
            # Exercise R2
            meta = None
            policy = None
            userKey = ("test", "users", username)
            (key, metadata,userRecord) = self.client.get(userKey,policy)
            if userRecord:                
                #  Get new password
                password = (raw_input("Enter new password for " + username + ":"))

                #  Note: Registration via udf_put() will register udfs both on server
                #  side and local client side in local user_path specified in connection
                #  configuration. AQL registers udfs with server only. If using AQL, 
                #  for stream udfs, copy them manually in local client node lua user_path.

                #  NOTE: UDF registration has been included here for convenience 
                #  and to demonstrate the syntax. 
                #  Create a separate script to register udfs only when modified.

                # Exercise R2
                self.client.udf_put(lua_file_name, udf_type, policy)
                time.sleep(5)
                # Execute UDF
                # Exercise R2
                updatedPassword = self.client.apply(userKey, "updateUserPwd", "updatePassword", [password])

                # Output updated password to the console
                # Exercise R2
                print("\nINFO: The password has been set to: " , updatedPassword)
            else:
                print("ERROR: User record not found!")
        else:
            print("ERROR: User record not found!")

    def updatePasswordUsingCAS(self):
        userRecord = None
        userKey = None
        passwordBin = None
        #  Get username
        username = str()
        username = raw_input("Enter username: ")
        if len(username) > 0:
            #  Check if username exists
            meta = None
            policy = None
            userKey = ("test", "users", username)
            (key, metadata,userRecord) = self.client.get(userKey,policy)
            if userRecord:
                record = {}
                #  Get new password
                record["password"] = raw_input("Enter new password for " + username + ":")
                # Exercise K5                                
                #  Save record generation
                #TODO: Save generation value of record read in usergen
                
                # Set Policy
                #TODO: Set gen policy to EQUAL
                
                # Set Meta data of record to update
                #TODO: Set gen metadata of record to write                
                
                try:
                  #TODO: Write record with userKey, record, meta and policy
                #TODO: Process RecordGenerationError exception
                  print("put() failed due to generation policy mismatch")
                except AerospikeError as e:
                  print("Error: {0} [{1}]".format(e.msg, e.code))
                print("\nINFO: The password has been set to: " , record["password"])
            else:
                print("ERROR: User record not found!")
        else:
            print("ERROR: User record not found!")

    def batchGetUserTweets(self):
        userRecord = None
        userKey = None
        #  Get username
        username = str()
        username = raw_input("Enter username: ")
        if len(username) > 0:
            #  Check if username exists
            # Exercise K3
            meta = None
            policy = None
            userKey = ("test", "users", username)
            (key, metadata,userRecord) = self.client.get(userKey,policy)
            if userRecord:
                # Get how many tweets the user has
                # Exercise K3
                print("\nGet how many tweets the user has.")
                #TODO: Get tweetCount from userRecord
                # Exercise K3
                # Create an array of tweet keys -- keys[tweetCount]
                keys = []                
                for i in range(1, tweetCount+1):                    
                    #TODO: Construct tweetKey string         
                    #TODO: Append tweet key tuple to keys array.
                    pass
                print("\nCreate an array of Key instances -- keys[tweetCount]")
                # Initiate batch read operation
                # Exercise K3
                print("\nInitiate batch read operation");
                #TODO: Batch read records using get_many() and passing array of keys.
                #Note: get_many() API returns a list of tuples (key, meta, bins)
                # Output tweets to the console
                # Exercise K3
                print("\nOutput tweets to the console");
                for i in range(1, tweetCount+1):
                    #TODO: Take the bin component of the tuple and print 'tweet'
                    pass

            else:
                print("ERROR: User record not found!\n")
        else:
            print("ERROR: Invalid User name.\n")

    def aggregateUsersByTweetCountByRegion(self):
        policy = {}
        udf_type = 0 # 0 for LUA
        lua_file_name = 'udf/aggregationByRegion.lua'
        try:
            min = int(raw_input("Enter Min Tweet Count: "))
            max = int(raw_input("Enter Max Tweet Count: "))
            print("\nAggregating users with " , min , "-" , max , " tweets by region:\n")

            # Register UDF
            # Exercise A2
            #  Note: Registration via udf_put() will register udfs both on server
            #  side and local client side in local user_path specified in connection
            #  configuration. AQL registers udfs with server only. If using AQL,
            #  for stream udfs, copy them manually in local client node lua user_path.

            #  NOTE: UDF registration has been included here for convenience
            #  and to demonstrate the syntax.
            #  Create a separate script to register udfs only when modified.

            self.client.udf_put(lua_file_name, udf_type, policy)
            time.sleep(5)
            
            # Create a Secondary Index on tweetcount
            # Preferred way to create a Secondary Index is via AQL
            # Exercise A2
            self.client.index_integer_create("test", "users", "tweetcount", "tweetcount_index", None)
            time.sleep(5)  #give time to build the index
            print("\nNumeric Secondary Index on tweetcount Created ")
            

            #Create query
            # Exercise A2
            tweetQuery = self.client.query("test", "users")

            # Set min--max range Filter on tweetcount
            # Exercise A2
            tweetQuery.where(p.between('tweetcount',min,max))

            # Execute aggregate query passing in , .lua filename of the UDF and lua function name
            # Exercise A2
            tweetQuery.apply("aggregationByRegion", "sum")

            # Define callback to Output result to the console in format \"Total Users in <region>: <#>\"
            # Exercise A2            
            def tweetQueryAggCallback(resultMap):
              print("\nTotal Users in North: ", resultMap['n'],"\n")
              print("\nTotal Users in South: ", resultMap['s'],"\n")
              print("\nTotal Users in East: ", resultMap['e'],"\n")
              print("\nTotal Users in West: ", resultMap['w'],"\n")


            # Execute the query and for each result invoke the callback
            # Note: We expect a single result of type map in this example.
            # Exercise A2
            tweetQuery.foreach(tweetQueryAggCallback)

        except Exception as e :
            print("error: {0}".format(e), file=sys.stderr)


    def createUsers(self):
        """ generated source for method createUsers """
        genders = ["m", "f"]
        regions = ["n", "s", "e", "w"]
        randomInterests = ["Music", "Football", "Soccer", "Baseball", "Basketball", "Hockey", "Weekend Warrior", "Hiking", "Camping", "Travel", "Photography"]
        username = str()
        userInterests = None
        totalInterests = 0
        start = 1
        end = 10000
        totalUsers = end - start + 1
        wr_policy = {'exists':aerospike.POLICY_EXISTS_IGNORE}
        print("\nCreate " , totalUsers , " users. Press any key to continue...\n")
        raw_input("..")
        j = start
        while j <= end:
            username = "user" + str(j)
            meta = None
            key = ("test", "users", username)
            record = {}
            record["username"] = username
            record["password"] = 'pwd' + str(j)
            record["gender"] = random.choice(genders)
            record["region"] = random.choice(regions)
            record["lasttweeted"] = 0
            record["tweetcount"] = 0
            record["interests"] = randomInterests[:random.randint(1,9)]
            self.client.put(key,record,meta,wr_policy)
            print("Wrote user record for " , username , "\n")
            j += 1
        #  Write user record
        print("\nDone creating " , totalUsers , " users!\n")


