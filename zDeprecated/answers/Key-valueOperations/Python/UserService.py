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
AS_POLICY_W_EXISTS     = "exists"
AS_POLICY_EXISTS_UNDEF = None # Use default value
AS_POLICY_EXISTS_IGNORE= aerospike.POLICY_EXISTS_IGNORE # Write the record, regardless of existence.
AS_POLICY_EXISTS_CREATE= aerospike.POLICY_EXISTS_CREATE # Create a record, ONLY if it doesn't exist.
AS_POLICY_EXISTS_UPDATE= aerospike.POLICY_EXISTS_UPDATE # Update a record, ONLY if it exists

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
        #  Get username
        username = raw_input("Enter username: ") 
        record = { "username": username }
        if len(username) > 0:
            #  Get password
            record['password'] = raw_input("Enter password for " + username + ":") 
            #  Get gender
            record['gender'] = raw_input("Select gender (f or m) for " + username + ":") 
            #  Get region
            record['region'] = raw_input("Select region (north, south, east or west) for " + username + ":") 
            #  Get interests
            record['interests'] = raw_input("Enter comma-separated interests for " + username + ":").split(',')
            #  Write record
            # Exercise 2
            #wPolicy.recordExistsAction = RecordExistsAction.UPDATE
            meta = None
            policy = None
            self.client.put(("test", "users", username),record,meta,policy)
            print(record, "\nINFO: User record created!")

    def getUser(self):
        userRecord = None
        userKey = None
        #  Get username
        username = str()
        username = raw_input("Enter username: ")
        if len(username) > 0:
            #  Check if username exists
            # Exercise 2
            meta = None
            policy = None
            userKey = ("test", "users", username)
            (key, metadata,userRecord) = self.client.get(userKey,policy)
            if userRecord:
                print("\nINFO: User record read successfully! Here are the details:\n")
                # Exercise 2
                print("username:   " , userRecord["username"] , "\n")
                print("password:   " , userRecord["password"] , "\n")
                print("gender:     " , userRecord["gender"] , "\n")
                print("region:     " , userRecord["region"] , "\n")
                print("tweetcount: ", userRecord["tweetcount"],"\n")
                print("interests:  " , userRecord["interests"] , "\n")
            else:
                print("ERROR: User record not found!\n")
        else:
            print("ERROR: User record not found!\n")


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
                # Exercise 5
                #  record generation
                usergen = metadata['gen']
                #print("usergen ="+str(usergen))
                policy = {'gen':aerospike.POLICY_GEN_EQ}
                meta = {'gen':usergen}
                try:
                  self.client.put(userKey,record,meta,policy)
                except RecordGenerationError:
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
            # Exercise 3
            meta = None
            policy = None
            userKey = ("test", "users", username)
            (key, metadata,userRecord) = self.client.get(userKey,policy)
            if userRecord:
                # Get how many tweets the user has
                # Exercise 3
                print("\nGet how many tweets the user has")
                tweetCount = userRecord['tweetcount']
                # Exercise 3
                # Create an array of tweet keys -- keys[tweetCount]
                keys = []
                pkey = []
                for i in range(1, tweetCount+1):
                    pkey.append(userRecord['username']+':'+str(i))
                    keys.append( ('test', 'tweets', pkey[i-1]))
                print("\nCreate an array of Key instances -- keys[tweetCount]")
                # Initiate batch read operation
                # Exercise 3
                print("\nInitiate batch read operation");
                records = self.client.get_many(keys)
                #Note: get_many() API returns a list of tuples (key, meta, bins)
                # Output tweets to the console
                # Exercise 3
                print("\nOutput tweets to the console");
                for i in range(1, tweetCount+1):
                    print(records[i-1][2]['tweet'])

            else:
                print("ERROR: User record not found!\n")
        else:
            print("ERROR: Invalid User name.\n")

    def aggregateUsersByTweetCountByRegion(self):
        """ generated source for method aggregateUsersByTweetCountByRegion """


    def createUsers(self):
        """ generated source for method createUsers """
        genders = ["m", "f"]
        regions = ["n", "s", "e", "w"]
        randomInterests = ["Music", "Football", "Soccer", "Baseball", "Basketball", "Hockey", "Weekend Warrior", "Hiking", "Camping", "Travel", "Photography"]
        username = str()
        userInterests = None
        totalInterests = 0
        start = 1
        end = 100000
        totalUsers = end - start
        wr_policy = {
                AS_POLICY_W_EXISTS:  AS_POLICY_EXISTS_IGNORE
        }
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
        print("\nDone creating " , totalUsers , "!\n")


