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
import time
from aerospike import predicates as p
import random

AS_POLICY_W_EXISTS     = "exists"
AS_POLICY_EXISTS_UNDEF = 0 # Use default value
AS_POLICY_EXISTS_IGNORE= 1 # Write the record, regardless of existence.
AS_POLICY_EXISTS_CREATE= 2 # Create a record, ONLY if it doesn't exist.
AS_POLICY_EXISTS_UPDATE= 3 # Update a record, ONLY if it exist (NOT YET IMPL).

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
        #  Get username
        username = str()
        username = raw_input("Enter username: ") 
        if len(username) > 0:
            #  Check if username exists
            meta = None
            policy = None
            userKey = ("test", "users", username)
            (key, metadata,userRecord) = self.client.get(userKey,policy)
            record = {}
            if userRecord:
                # Set Tweet Count 
                if 'tweetcount' in userRecord:
                  nextTweetCount = int(userRecord['tweetcount']) + 1
                else:
                  nextTweetCount = 1
                #  Get tweet
                record['tweet'] = raw_input("Enter tweet for " + username + ":")
                #  Write record
                #wPolicy.recordExistsAction = RecordExistsAction.UPDATE
                #  Create timestamp to store along with the tweet so we can
                #  query, index and report on it
                ts= self.getTimeStamp()
                tweetKey = ("test", "tweets", username + ":" + str(nextTweetCount))
                record["ts"] =  ts
                record["username"]= username
                self.client.put(tweetKey,record, meta, policy) 
                print("\nINFO: Tweet record created!\n",record,tweetKey)
                #  Update tweet count and last tweet'd timestamp in the user
                #  record
                self.updateUser(self.client, userKey, policy, ts, nextTweetCount)
            else:
                print("ERROR: User record not found!\n")

    def scanAllTweetsForAllUsers(self):
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
        userTweet = {}
        userTweet["tweetcount"] = tweetCount
        userTweet["lasttweeted"] = ts
	meta = None
        self.client.put(userKey,userTweet, meta, policy)
        print("\nINFO: The tweet count now is: " , tweetCount)

    def updateUserUsingOperate(self, client, userKey, policy, ts):
        """ operate not supported in Python Client """
        print("\nINFO: The tweet count now is: ")

    def queryTweetsByUsername(self):
        print("\n********** Query Tweets By Username **********\n")
        #  Get username
        username = str()
        username = raw_input("Enter username: ")
        if len(username) > 0:
          try:
            #  Python Scan
            tweetQuery = self.client.query("test", "tweets")
            tweetQuery.select('username','tweet')
            # callback for each record read
            def tweetQueryCallback((key, meta, record)):
              print(record)
            # invoke the operations, and for each record invoke the callback
            tweetQuery.where(p.equals('username',username))
            tweetQuery.foreach(tweetQueryCallback)
          except Exception as e :
            print("error: {0}".format(e), file=sys.stderr)

    # queryTweetsByUsername
    def queryUsersByTweetCount(self):
        print("\n********** Query Users By Tweet Count Range **********\n")


    def getTimeStamp(self):
        return int(round(time.time() * 1000)) 

    def createTweets(self):
        randomTweets = ["For just $1 you get a half price download of half of the song and listen to it just once.", "People tell me my body looks like a melted candle", "Come on movie! Make it start!", "Byaaaayy", "Please, please, win! Meow, meow, meow!", "Put. A. Bird. On. It.", "A weekend wasted is a weekend well spent", "Would you like to super spike your meal?", "We have a mean no-no-bring-bag up here on aisle two.", "SEEK: See, Every, EVERY, Kind... of spot", "We can order that for you. It will take a year to get there.", "If you are pregnant, have a soda.", "Hear that snap? Hear that clap?", "Follow me and I may follow you", "Which is the best cafe in Portland? Discuss...", "Portland Coffee is for closers!", "Lets get this party started!", "How about them portland blazers!", "You got school'd, yo", "I love animals", "I love my dog", "What's up Portland", "Which is the best cafe in Portland? Discuss...", "I dont always tweet, but when I do it is on Tweetaspike"]
        totalUsers = 10000
        maxTweets = 20
        username = str()
        ts = 0
        wr_policy = {
                AS_POLICY_W_EXISTS:  AS_POLICY_EXISTS_IGNORE
        }
        print("\nCreate up to " , maxTweets , " tweets each for " , totalUsers , " users. Press any key to continue...\n")
        raw_input("..")
        j = 0
        while j < totalUsers:
            username = "user" + str(random.randint(1,totalUsers))
            userKey = ("test", "users", username)
            meta = None
            policy = None
            ts = None
            k = 0
            (key, metadata,userRecord) = self.client.get(userKey,policy)
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
                print("\nWrote " , totalTweets , " tweets for " , username , "!")
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
