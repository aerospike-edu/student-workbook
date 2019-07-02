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
from builtins import input
import aerospike
from aerospike import exception
import sys
import time
from aerospike import predicates as p
import random


class TweetService(object):

    def __init__(self, client):
        self.client = client


    def createTweet(self):
        print("\n********** Create Tweet **********\n")
        # /*********************///
        # /*****Data Model*****///
        # Namespace: test
        # Set: tweets
        # Key: <username:<counter>>
        # Bins:
        # tweet - string 
        # ts - int (Stores epoch timestamp of the tweet)
        # username - string
        # Sample Key: dash:1
        # Sample Record:
        # { tweet: 'Put. A. Bird. On. It.',
        #   ts: 1408574221,
        #   username: 'dash'
        # }
        # /*********************///
        userRecord = None
        userKey = None
        tweetKey = None
        # Get username
        username = str()
        username = input("Enter username: ") 
        if len(username) > 0:
            # Check if username exists
            # Exercise K2
            print("\nCheck if username exists")
            meta = None
            policy = None
            record = {}
            reckey = ('test', 'users', username)
            (reckey, meta, userRecord) = self.client.get(reckey)
            if userRecord:
                # Set Tweet Count
                if 'tweetcount' in userRecord:
                  nextTweetCount = int(userRecord['tweetcount']) + 1
                else:
                  nextTweetCount = 1
                #  Get tweet
                record['tweet'] = input("Enter tweet for " + username + ":")
                #  Create timestamp to store along with the tweet so we can
                #  query, index and report on it
                ts= self.getTimeStamp()
                # Create WritePolicy instance
                # Exercise K2
                print("\nCreate WritePolicy instance");

                #Set the 'exists' policy ie what to do if the record exists
                #to POLICY_EXISTS_CREATE - create record only if it does not exist
                policy = {'exists': aerospike.POLICY_EXISTS_CREATE}

                # Create Key and Bin instances for the tweet record.
                # Exercise K2                
                print("\nCreate Primary Key and Bin instances for the tweet record");
                #HINT: tweet key should be in username:nextTweetCount format
                tweetKey = ('test', 'tweets', username + ':'+ str(nextTweetCount))
                record['ts']=ts
                record['username']=username
                # Write tweet record
                # Exercise K2
                print("\nWrite tweet record");
                self.client.put(tweetKey, record, policy)
                # Update tweet count and last tweeted timestamp in the user record 
                # Exercise K2
                # We are updating an existing record                
                policy = {'timeout': 300}  #If connection is slow, increase the timeout
                userKey = ('test','users',username)
                
                #We could do below,
                
                #userRecord['lasttweeted']=ts
                #userRecord['tweetcount']=nextTweetCount                
                #client.put(userKey, userRecord, policy)
                
                #But let us pass these params and use the updateUser()
                
                self.updateUser(self.client, userKey, policy, ts, nextTweetCount)

            else:
                print("ERROR: User record not found!\n")


    def scanAllTweetsForAllUsers(self):
        # Initiate scan operation that invokes callback for outputting tweets on the console
        # Exercise K4  
        try:
            #  Python Scan
            tweetScan = self.client.scan("test", "tweets")
            tweetScan.select('tweet')
            # callback for each record read
            def tweetCallback((key, meta, record)):
              print(record)
            # invoke the operations, and for each record invoke the callback
            tweetScan.foreach(tweetCallback)
        except Exception as e :
            print("error: {0}".format(e), file=sys.stderr)

    def updateUser(self, client, userKey, policy, ts, tweetCount):
        # Update tweet count and last tweeted timestamp in the user record
        # Exercise K2  
        print("\nUpdate tweet count and last tweeted timestamp in the user record")
        userRecord = {}
        userRecord['lasttweeted']=ts
        userRecord['tweetcount']=tweetCount
        
        #Comment line below for Exercise K6
        client.put(userKey, userRecord, policy) 
        
        # Exercise K6, uncomment line below 
        #self.updateUserUsingOperate(client, userKey, policy, ts, tweetCount)

    def updateUserUsingOperate(self, client, userKey, policy, ts, tweetCount):
        """ operate now supported in Python Client """
        # User Operate() to set and get tweetcount
        # Exercise K6 

        ops= [{
          "op" : aerospike.OPERATOR_WRITE,
          "bin": "lasttweeted",
          "val": ts
        },
        {
          "op" : aerospike.OPERATOR_WRITE,
          "bin": "tweetcount",
          "val": tweetCount
        },
        {
          "op" : aerospike.OPERATOR_READ,
          "bin": "tweetcount"
        }]
        meta = {}
        (key, meta, bins) = self.client.operate(userKey, ops, meta, policy)

        print("\nOperate(): The tweet count now is: " + str(bins['tweetcount']))


    def queryTweetsByUsername(self):
        print("\n********** Query Tweets By Username **********\n")
        #  Get username
        username = str()
        username = input("Enter username: ")
        if len(username) > 0:
          try:
            # Create a Secondary Index on username
            # Exercise Q3
            #self.client.index_string_create("test", "tweets", "username", "username_index", None)
            #time.sleep(5)
            #print("\nString Secondary Index Created ")

            # Create Query and Set equality Filter on username
            # Exercise Q3
            tweetQuery = self.client.query("test", "tweets")
            # Select bin(s) you would like to retrieve
            tweetQuery.select('tweet')            
            tweetQuery.where(p.equals('username',username))

            # Define the Call back to print Tweets for given Username
            # Exercise Q3
            def tweetQueryCallback((key, meta, record)):
              print(record["tweet"])

            # Execute query and for each record invoke the callback
            # Exercise Q3
            tweetQuery.foreach(tweetQueryCallback)
          except Exception as e :
            print("error: {0}".format(e), file=sys.stderr)
            
    def queryUsersByTweetCount(self):
        print("\n********** Query Users By Tweet Count Range **********\n")
        try:
            # Create a Secondary Index on tweetcount
            # Exercise Q4
            #self.client.index_integer_create("test", "users", "tweetcount", "tweetcount_index", None)
            #time.sleep(5)
            #print("\nInteger Secondary Index Created ")

            # Create Query and Set min--max range Filter on tweetcount
            # Exercise Q4
            min = int(input("Enter Min Tweet Count: "))
            max = int(input("Enter Max Tweet Count: "))
            print("\nList of users with " , min , "-" , max , " tweets:\n")
            
            tweetQuery = self.client.query("test", "users")
            # Select bin(s) you would like to retrieve
            tweetQuery.select('username', 'tweetcount')             
            tweetQuery.where(p.between('tweetcount',min,max))

            # Define the Call back to print Tweets for given Username
            # Exercise Q4
            
            def tweetQueryCountCallback((key, meta, record)):
              print(record["username"] , " has " , record["tweetcount"] , " tweets\n")

            # Execute query and for each record invoke the callback
            # Exercise Q4            
            tweetQuery.foreach(tweetQueryCountCallback)
        except Exception as e :
            print("error: {0}".format(e), file=sys.stderr)


    def getTimeStamp(self):
        return int(round(time.time() * 1000)) 


    def createTweets(self):
        randomTweets = ["For just $1 you get a half price download of half of the song and listen to it just once.", "People tell me my body looks like a melted candle", "Come on movie! Make it start!", "Byaaaayy", "Please, please, win! Meow, meow, meow!", "Put. A. Bird. On. It.", "A weekend wasted is a weekend well spent", "Would you like to super spike your meal?", "We have a mean no-no-bring-bag up here on aisle two.", "SEEK: See, Every, EVERY, Kind... of spot", "We can order that for you. It will take a year to get there.", "If you are pregnant, have a soda.", "Hear that snap? Hear that clap?", "Follow me and I may follow you", "Which is the best cafe in Portland? Discuss...", "Portland Coffee is for closers!", "Lets get this party started!", "How about them portland blazers!", "You got school'd, yo", "I love animals", "I love my dog", "What's up Portland", "Which is the best cafe in Portland? Discuss...", "I dont always tweet, but when I do it is on Tweetaspike"]
        totalUsers = 1000
        maxUsers = 10000
        maxTweets = 20
        username = str()
        ts = 0
        wr_policy = {'exists':aerospike.POLICY_EXISTS_IGNORE}                
        print("\nCreate up to " , maxTweets , " tweets each for " , totalUsers , " users. Press any key to continue...\n")
        input("..")
        j = 0
        while j < totalUsers:
            username = "user" + str(random.randint(1,maxUsers))
            userKey = ("test", "users", username)
            meta = None
            policy = None
            ts = None
            k = 0
            (key, metadata,userRecord) = self.client.get(userKey,policy)
            #Note: Demo code. May update same userRecord more than once. Expect ~50% unique updates.
            if userRecord:
                totalTweets = random.randint(1,maxTweets)
                while k <= totalTweets:
                    record = {}
                    ts = self.getTimeStamp()
                    tweetKey = ("test", "tweets", username + ":" + str(k))
                    record["tweet"] = random.choice(randomTweets)
                    record["ts"] =  ts
                    record["username"]= username
                    self.client.put(tweetKey,record, meta, wr_policy)
                    k += 1
                #  Create timestamp to store along with the tweet so we can
                #  query, index and report on it
                print("\nWrote " , totalTweets , " tweets for " , username)
                if totalTweets > 0:
                    #  Update tweet count and last tweet'd timestamp in the user
                    #  record
                    self.updateUser(self.client, userKey, wr_policy, ts, totalTweets)
            j += 1
        #  Check if user record exists
        #  create up to maxTweets random tweets for this user
        #  Create timestamp to store along with the tweet so we can
        #  query, index and report on it
        #  Update tweet count and last tweet'd timestamp in the user
        #  record
        print("\n\nDone creating up to " , maxTweets , " tweets each for " , totalUsers , " users!\n")


