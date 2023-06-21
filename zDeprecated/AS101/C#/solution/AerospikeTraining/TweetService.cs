/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Aerospike.Client;

namespace AerospikeTraining
{
    class TweetService
    {
        private AerospikeClient client;

        public TweetService(AerospikeClient c)
        {
            this.client = c;
        }

        public void createTweet()
        {
            Console.WriteLine("\n********** Create Tweet **********\n");

            ///*********************///
            ///*****Data Model*****///
            //Namespace: test
            //Set: tweets
                //Key: <username:<counter>>
                //Bins:
                    //tweet - string 
                    //ts - int (Stores epoch timestamp of thetweet)
                    //username - string

                //Sample Key: dash:1
                //Sample Record:
                    //{ tweet: 'Put. A. Bird. On. It.',
                    //  ts: 1408574221,
                    //  username: 'dash'
                    //}
            ///*********************///

            Record userRecord = null;
            Key userKey = null;
            Key tweetKey = null;

            // Get username
            string username;
            Console.WriteLine("\nEnter username:");
            username = Console.ReadLine();

            if (username != null && username.Length > 0)
            {
                // TODO: Read user record and check if it exists
                // Exercise K2
                userKey = new Key("test", "users", username);
                userRecord = client.Get(null, userKey);
                if (userRecord != null)
                {
                    int nextTweetCount = int.Parse(userRecord.GetValue("tweetcount").ToString()) + 1;

                    // Get tweet
                    string tweet;
                    Console.WriteLine("Enter tweet for " + username + ":");
                    tweet = Console.ReadLine();

                    // TODO: Create WritePolicy instance
                    // Exercise K2
                    WritePolicy wPolicy = new WritePolicy();
                    wPolicy.recordExistsAction = RecordExistsAction.UPDATE;

                    // Create timestamp to store along with the tweet so we can query, index and report on it
                    long ts = getTimeStamp();

                    // TODO: Create Key and Bin instances for the tweet record
                    // Hint: tweet key should be in username:nextTweetCount format
                    // Exercise K2
                    tweetKey = new Key("test", "tweets", username + ":" + nextTweetCount);

                    Bin bin1 = new Bin("tweet", tweet);
                    Bin bin2 = new Bin("ts", ts);
                    Bin bin3 = new Bin("username", username);

                    // TODO: Write tweet record
                    // Exercise K2
                    client.Put(wPolicy, tweetKey, bin1, bin2, bin3);
                    Console.WriteLine("\nINFO: Tweet record created!");

                    // TODO: Update tweet count and last tweet'd timestamp in the 
                    // user record. Add code in updateUser()
                    // Exercise K2
                    updateUser(client, userKey, wPolicy, ts, nextTweetCount);
                }
                else
                {
                    Console.WriteLine("ERROR: User record not found!");
                }
            }
        } //createTweet

        private void updateUser(AerospikeClient client, Key userKey, WritePolicy policy, long ts, int tweetCount)
        {            
            // TODO: Update tweet count and last tweet'd timestamp in the user record
            // Exercise K2
            Bin bin1 = new Bin("tweetcount", tweetCount);
            Bin bin2 = new Bin("lasttweeted", ts);
            client.Put(policy, userKey, bin1, bin2);
            Console.WriteLine("INFO: The tweet count now is: " + tweetCount);

            // Exercise K6
            // Comment above code and uncomment call below to updateUserUsingOperate()
            // TODO: Add code in updateUserUsingOperate()
            // updateUserUsingOperate(client, userKey, policy, ts);
        } //updateUser

        private void updateUserUsingOperate(AerospikeClient client, Key userKey, WritePolicy policy, long ts)
        {
            // TODO: Initiate operate passing in policy, user record key,
            // .Add operation incrementing tweet count, .Put operation updating timestamp
            // and .Get operation to read the user record.
            // Exercise K6
            Record record = client.Operate(policy, userKey, Operation.Add(new Bin("tweetcount", 1)), Operation.Put(new Bin("lasttweeted", ts)), Operation.Get());
            
            // TODO: Output the most recent tweetcount
            // Exercise K6
            Console.WriteLine("INFO: The tweet count now is: " + record.GetValue("tweetcount"));
        } //updateUserUsingOperate

        public void scanAllTweetsForAllUsers()
        {
            // Scan all records
            // TODO: Create ScanPolicy instance with concurrentNodes, LOW priority and includeBinData
            // Exercise K4
            ScanPolicy policy = new ScanPolicy();
            policy.concurrentNodes = true;
            policy.priority = Priority.LOW;
            policy.includeBinData = true;

            // TODO: Initiate scan operation that invokes callback for outputting tweets to the console.
            // Exercise K4
            client.ScanAll(policy, "test", "tweets", scanTweetsCallback, "tweet");
        } //scanAllTweets

        public void scanTweetsCallback(Key key, Record record)
        {
            // TODO: Output the tweet in the record.
            // Exercise K4
            Console.WriteLine(record.GetValue("tweet"));
        } //scanTweetsCallback

        public void queryTweetsByUsername()
        {
            Console.WriteLine("\n********** Query Tweets By Username **********\n");

            // TODO: Create STRING index on username in tweets set
            // Exercise Q1
            // NOTE: Index creation has been included in here for convenience and to demonstrate the syntax
            // The recommended way of creating indexes in production env is via AQL
            // or create once using a standalone application.
            //IndexTask task = client.CreateIndex(null, "test", "tweets", "username_index", "username", IndexType.STRING);
            //task.Wait();

            RecordSet rs = null;
            try
            {
                // Get username
                string username;
                Console.WriteLine("\nEnter username:");
                username = Console.ReadLine();

                if (username != null && username.Length > 0)
                {
                    // TODO: Create string array of bins that you would like to retrieve
                    // In this example, we want to display all tweets for a given user.
                    // Exercise Q3

                    string[] bins = { "tweet" };

                    // TODO: Create Statement instance
                    // Exercise Q3
                    Statement stmt = new Statement();

                    // TODO: Set namespace on the instance of the Statement
                    // Exercise Q3
                    stmt.SetNamespace("test");

                    // TODO: Set the name of the set on the instance of the Statement
                    // Exercise Q3
                    stmt.SetSetName("tweets");

                    // TODO: Set the name of index on the instance of the Statement
                    // Exercise Q3
                    stmt.SetIndexName("username_index");

                    // TODO: Set the list of bins to retrieve on the instance of the Statement
                    // Exercise Q3
                    stmt.SetBinNames(bins);

                    // TODO: Set the equality Filter on username on the instance of the Statement
                    // Exercise Q3
                    stmt.SetFilters(Filter.Equal("username", username));

                    Console.WriteLine("\nHere's " + username + "'s tweet(s):\n");

                    // TODO: Execute the Query passing null policy and Statement instance
                    // Exercise Q3
                    rs = client.Query(null, stmt);


                    while (rs.Next())
                    {
                        // TODO: Iterate through returned RecordSet and output tweets to the console.
                        // Exercise Q3
                        Record r = rs.Record;
                        Console.WriteLine(r.GetValue("tweet"));
                    }
                }
                else
                {
                    Console.WriteLine("ERROR: User record not found!");
                }
            }
            finally
            {
                // TODO: Close the RecordSet
                // Exercise Q3
                if (rs != null)
                {
                    // Close record set
                    rs.Close();
                }
            }
        } //queryTweetsByUsername

        public void queryUsersByTweetCount()
        {
            Console.WriteLine("\n********** Query Users By Tweet Count Range **********\n");

            // TODO: Create NUMERIC index on tweetcount in users set
            // Exercise Q2
            // NOTE: Index creation has been included in here for convenience and to demonstrate the syntax
            // The recommended way of creating indexes in production env is via AQL
            // or create once using a standalone application.
            //IndexTask task = client.CreateIndex(null, "test", "users", "tweetcount_index", "tweetcount", IndexType.NUMERIC);
            //task.Wait();

            RecordSet rs = null;
            try
            {                
                // Get min and max tweet counts
                int min;
                int max;
                Console.WriteLine("\nEnter Min Tweet Count:");
                min = int.Parse(Console.ReadLine());
                Console.WriteLine("Enter Max Tweet Count:");
                max = int.Parse(Console.ReadLine());

                // TODO: Create string array of bins that you would like to retrieve
                // In this example, we want to display which user has how many tweets.
                // Exercise Q4
                string[] bins = { "username", "tweetcount" };

                // TODO: Create Statement instance
                // Exercise Q4
                Statement stmt = new Statement();

                // TODO: Set namespace on the instance of the Statement
                // Exercise Q4
                stmt.SetNamespace("test");

                // TODO: Set the name of the set on the instance of the Statement
                // Exercise Q4
                stmt.SetSetName("users");

                // TODO: Set the name of index on the instance of the Statement
                // Exercise Q4
                stmt.SetIndexName("tweetcount_index");

                // TODO: Set the list of bins to retrieve on the instance of the Statement
                // Exercise Q4
                stmt.SetBinNames(bins);

                // TODO: Set the range Filter on tweetcount on the instance of the Statement
                // Exercise Q4
                stmt.SetFilters(Filter.Range("tweetcount", min, max));


                Console.WriteLine("\nList of users with " + min + "-" + max + " tweets:\n");

                // TODO: Execute the Query passing null policy and Statement instance
                // Exercise Q4
                rs = client.Query(null, stmt);

                while (rs.Next())
                {
                    // TODO: Iterate through returned RecordSet and output text in format "<username> has <#> tweets"
                    // Exercise Q4
                    Record r = rs.Record;
                    Console.WriteLine(r.GetValue("username") + " has " + r.GetValue("tweetcount") + " tweets");
                }
            }
            finally
            {
                // TODO: Close the RecordSet
                // Exercise Q4
                if (rs != null)
                {
                    // Close record set
                    rs.Close();
                }
            }
        } //queryUsersByTweetCount

        public void createTweets()
        {
            string[] randomTweets = { "For just $1 you get a half price download of half of the song and listen to it just once.", "People tell me my body looks like a melted candle", "Come on movie! Make it start!", "Byaaaayy", "Please, please, win! Meow, meow, meow!", "Put. A. Bird. On. It.", "A weekend wasted is a weekend well spent", "Would you like to super spike your meal?", "We have a mean no-no-bring-bag up here on aisle two.", "SEEK: See, Every, EVERY, Kind... of spot", "We can order that for you. It will take a year to get there.", "If you are pregnant, have a soda.", "Hear that snap? Hear that clap?", "Follow me and I may follow you", "Which is the best cafe in Portland? Discuss...", "Portland Coffee is for closers!", "Lets get this party started!", "How about them portland blazers!", "You got school'd, yo", "I love animals", "I love my dog", "What's up Portland", "Which is the best cafe in Portland? Discuss...", "I dont always tweet, but when I do it is on Tweetaspike" };
            Random rnd1 = new Random();
            Random rnd2 = new Random();
            Random rnd3 = new Random();
            Key userKey;
            Record userRecord;
            int totalUsers = 1000;
            int maxUsers = 10000;
            int maxTweets = 20;
            string username;
            long ts = 0;
            WritePolicy wPolicy = new WritePolicy();
            wPolicy.recordExistsAction = RecordExistsAction.UPDATE;

            Console.WriteLine("\nCreate up to " + maxTweets + " tweets each for " + totalUsers + " users. Press any key to continue...");
            Console.ReadLine();

            for (int j = 0; j < totalUsers; j++)
            {
                // Check if user record exists
                username = "user" + rnd3.Next(1, maxUsers);
                userKey = new Key("test", "users", username);
                userRecord = client.Get(null, userKey);
                if (userRecord != null)
                {
                    // create up to maxTweets random tweets for this user
                    int totalTweets = rnd1.Next(1, (maxTweets + 1));
                    for (int k = 1; k <= totalTweets; k++)
                    {
                        // Create timestamp to store along with the tweet so we can query, index and report on it
                        ts = getTimeStamp();
                        Key tweetKey = new Key("test", "tweets", username + ":" + k);
                        Bin bin1 = new Bin("tweet", randomTweets[rnd2.Next(1, randomTweets.Length)]);
                        Bin bin2 = new Bin("ts", ts);
                        Bin bin3 = new Bin("username", username);

                        client.Put(wPolicy, tweetKey, bin1, bin2, bin3);
                    }
                    if (totalTweets > 0)
                    {
                        // Update tweet count and last tweet'd timestamp in the user record
                        client.Put(wPolicy, userKey, new Bin("tweetcount", totalTweets), new Bin("lasttweeted", ts));
                        //Console.WriteLine("INFO: The tweet count now is: " + totalTweets);
                    }
                    Console.WriteLine("Wrote " + totalTweets + " tweets for " + username);
                }
            }
            Console.WriteLine("\nDone creating up to " + maxTweets + " tweets each for " + totalUsers + " users!");
        } //createTweets

        private long getTimeStamp()
        {
            double epoch = (DateTime.UtcNow - new DateTime(1970, 1, 1)).TotalSeconds;
            return Convert.ToInt64(epoch);
        } //getTimeStamp

    }
}
