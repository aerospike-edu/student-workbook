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
using Aerospike.Client;
using System.IO;

namespace AerospikeTraining
{
    class UserService
    {
        private AerospikeClient client;

        public UserService(AerospikeClient c)
        {
            this.client = c;
        }

        public void createUser()
        {
            Console.WriteLine("\n********** Create User **********\n");

            ///*********************///
            ///*****Data Model*****///
            //Namespace: test
            //Set: Users
                //Key: <username>
                //Bins:
                    //username - string
                    //password - string (For simplicity password is stored in plain-text)
                    //gender - string (Valid values are 'm' or 'f')
                    //region - string (Valid values are: 'n' (North), 's' (South), 'e' (East), 'w' (West) -- to keep data entry to minimal we just store the first letter)
                    //lasttweeted - Integer (Stores epoch timestamp of the last/most recent tweet) -- Default to 0
                    //tweetcount - Integer (Stores total number of tweets for the user) -- Default to 0
                    //interests - Array of interests) 

                //Sample Key: dash
                //Sample Record:
                    //{ username: 'dash',
                    //  password: 'dash',
                    //  gender: 'm',
                    //  region: 'w',
                    //  lasttweeted: 1408574221,
                    //  tweetcount: 20,
                    //  interests: ['photography', 'technology', 'dancing', 'house music] 
                    //}
            ///*********************///

            string username;
            string password;
            string gender;
            string region;
            string interests;

            // Get username
            Console.WriteLine("Enter username: ");
            username = Console.ReadLine();

			if (username != null && username.Length > 0)
            {
                // Get password
                Console.WriteLine("Enter password for " + username + ":");
                password = Console.ReadLine();

                // Get gender
                Console.WriteLine("Select gender (f or m) for " + username + ":");
                gender = Console.ReadLine().Substring(0, 1);

                // Get region
                Console.WriteLine("Select region (north, south, east or west) for " + username + ":");
                region = Console.ReadLine().Substring(0, 1);

                // Get interests
                Console.WriteLine("Enter comma-separated interests for " + username + ":");
                interests = Console.ReadLine();

                // TODO: Create WritePolicy instance
                // Exercise K2
                WritePolicy wPolicy = new WritePolicy();
                wPolicy.recordExistsAction = RecordExistsAction.UPDATE;

                //TODO: Create Key and Bin instances for the user record.
                //Remeber to conver comma-separated interests into a list before storing it
                // Exercise K2
                Key key = new Key("test", "users", username);
                Bin bin1 = new Bin("username", username);
                Bin bin2 = new Bin("password", password);
                Bin bin3 = new Bin("gender", gender);
                Bin bin4 = new Bin("region", region);
                Bin bin5 = new Bin("lasttweeted", 0);
                Bin bin6 = new Bin("tweetcount", 0);
                Bin bin7 = new Bin("interests", interests.Split(',').ToList<object>());

                // TODO: Write the user record
                // Exercise K2
                client.Put(wPolicy, key, bin1, bin2, bin3, bin4, bin5, bin6, bin7);

                Console.WriteLine("\nINFO: User record created!");
            }
        } //createUser

        public void getUser()
        {
            Record userRecord = null;
            Key userKey = null;

            // Get username
            string username;
            Console.WriteLine("\nEnter username:");
            username = Console.ReadLine();

            if (username != null && username.Length > 0)
            {
                // TODO: Read userRecord and check if it exists
                // Exercise K2
                userKey = new Key("test", "users", username);
                userRecord = client.Get(null, userKey);
                if (userRecord != null)
                {
                    // TODO: Output user record to the console
                    // Remember to convert list into comma-separated interests before outputting it.
                    // Exercise K2
                    Console.WriteLine("\nINFO: User record read successfully! Here are the details:\n");
                    Console.WriteLine("username:     " + userRecord.GetValue("username"));
                    Console.WriteLine("password:     " + userRecord.GetValue("password"));
                    Console.WriteLine("gender:       " + userRecord.GetValue("gender"));
                    Console.WriteLine("region:       " + userRecord.GetValue("region"));
                    Console.WriteLine("tweetcount:   " + userRecord.GetValue("tweetcount"));
                    List<object> interests = (List<object>) userRecord.GetValue("interests");
                    Console.WriteLine("interests:    " + interests.Aggregate((x, y) => x + "," + y));
                }
                else
                {
                    Console.WriteLine("ERROR: User record not found!");
                }
            }
            else
            {
                Console.WriteLine("ERROR: Invalid user name.");
            }
        } //getUser

        public void batchGetUserTweets()
        {
            Record userRecord = null;
            Key userKey = null;

            // Get username
            string username;
            Console.WriteLine("\nEnter username:");
            username = Console.ReadLine();

            if (username != null && username.Length > 0)
            {
                // TODO: Read userRecord and check if it exists
                // Exercise K3
                userKey = new Key("test", "users", username);
                userRecord = client.Get(null, userKey);
                if (userRecord != null)
                {
                    // TODO: Get how many tweets the user has
                    // Exercise K3
                    int tweetCount = int.Parse(userRecord.GetValue("tweetcount").ToString());

                    // TODO: Create an array of tweet keys so we can initiate batch read operation
                    // Exercise K3
                    Key[] keys = new Key[tweetCount];
                    for (int i = 0; i < keys.Length; i++)
                    {
                        keys[i] = new Key("test", "tweets", (username + ":" + (i + 1)));
                    }

                    Console.WriteLine("\nHere's " + username + "'s tweet(s):\n");

                    // TODO: Initiate batch read operation
                    // Note: Batch read returns all records once they have been 
                    // read. Null is returned for records not found. (We should not have any)
                    // We expect upto max 20 tweets.
                    // Exercise K3
                    Record[] records = client.Get(null, keys);
                    for (int j = 0; j < records.Length; j++)
                    {
                        // TODO: Output tweets to the console
                        // Exercise K3
                        Console.WriteLine(records[j].GetValue("tweet"));
                    }
                }
                else
                {
                    Console.WriteLine("ERROR: User record not found!");
                }
            }
            else
            {
                Console.WriteLine("ERROR: Invalid user name.");
            }
        } //getUserTweets

        public void updatePasswordUsingUDF()
        {
            Record userRecord = null;
            Key userKey = null;       

            // Get username
            string username;
            Console.WriteLine("\nEnter username:");
            username = Console.ReadLine();

            if (username != null && username.Length > 0)
            {
                // TODO: Read userRecord and check if it exists
                // Exercise R2
                //userKey = ....
                //userRecord = ....
                if (userRecord != null)
                {
                    //Get new password
                    string password;
                    Console.WriteLine("Enter new password for " + username + ":"); 
                    password = Console.ReadLine();

                    // TODO: Update userRecord using UDF                   
                    // Exercise R2
                    // NOTE: UDF registration has been included here for convenience and to demonstrate the syntax. 
                    // The recommended way of registering UDFs in production env is via AQL
                    // or standalone application using code similar to below.
                    string luaDirectory = @"..\..\udf";
                    LuaConfig.PackagePath = luaDirectory + @"\?.lua";
                    //string filename = ....
                    //string path = ....
                    //RegisterTask rt = ....
                    //rt.Wait();

                    // TODO: Execute the UDF updatePassword.lua
                    // Exercise R2
                    //string updatedPassword =  ....
                    
                    // TODO: Output the updated passord returned by the UDF
                    // Exercise R2
                    Console.WriteLine("\nINFO: The password has been set to: " + ".... (updatedPassword)");
                }
                else
                {
                    Console.WriteLine("\nERROR: User record not found.");
                }
            }
            else
            {
                Console.WriteLine("\nERROR: Invalid user name.");
            }
        } //updatePasswordUsingUDF

        public void updatePasswordUsingCAS()
        {
            Record userRecord = null;
            Key userKey = null;
            Bin passwordBin = null;

            // Get username
            string username;
            Console.WriteLine("\nEnter username:");
            username = Console.ReadLine();

            if (username != null && username.Length > 0)
            {
                // TODO: Read userRecord and check if it exists
                // Exercise K5
                userKey = new Key("test", "users", username);
                userRecord = client.Get(null, userKey);
                if (userRecord != null)
                {
                    //Get new password
                    string password;
                    Console.WriteLine("Enter new password for " + username + ":");
                    password = Console.ReadLine();

                    // TODO: Update userRecord with new password only if generation is the same                    
                    // Exercise K5
                    // Create WritePolicy instance
                    WritePolicy writePolicy = new WritePolicy();
                    writePolicy.generation = userRecord.generation;
                    writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
                    // password bin
                    passwordBin = new Bin("password", password);
                    // update userRecord
                    client.Put(writePolicy, userKey, passwordBin);

                    Console.WriteLine("\nINFO: The password has been set to: " + password);
                }
                else
                {
                    Console.WriteLine("\nERROR: User record not found.");
                }
            }
            else
            {
                Console.WriteLine("\nERROR: Invalid user name.");
            }
        } //updatePasswordUsingCAS

        public void aggregateUsersByTweetCountByRegion()
        {
            // TODO: Create NUMERIC index on tweetcount in users set (Same as Exercise Q4)
            // Exercise A2
            // NOTE: Index creation has been included in here for convenience and to demonstrate the syntax
            // The recommended way of creating indexes in production env is via AQL
            // or create once using a standalone application.            
            IndexTask task = client.CreateIndex(null, "test", "users", "tweetcount_index", "tweetcount", IndexType.NUMERIC);
            task.Wait();

            ResultSet rs = null;
            try
            {
                int min;
                int max;
                Console.WriteLine("\nEnter Min Tweet Count:");
                min = int.Parse(Console.ReadLine());
                Console.WriteLine("Enter Max Tweet Count:");
                max = int.Parse(Console.ReadLine());

                // TODO: Register UDF
                // Exercise A2
                // NOTE: UDF registration has been included here for convenience and to demonstrate the syntax. 
                // The recommended way of registering UDFs in production env is via AQL
                // or standalone application using code similar to below.
                string luaDirectory = @"..\..\udf";
                LuaConfig.PackagePath = luaDirectory + @"\?.lua";

                string filename = "aggregationByRegion.lua";
                string path = Path.Combine(luaDirectory, filename);

                RegisterTask rt = client.Register(null, path, filename, Language.LUA);
                rt.Wait();

                // TODO: Create string array of bins that you would like to retrieve
                // In this example, we want to display which region has how many tweets.
                // Exercise A2
                string[] bins = { "tweetcount", "region" };

                // TODO: Create Statement instance
                // Exercise A2
                Statement stmt = new Statement();

                // TODO: Set namespace on the instance of the Statement
                // Exercise A2
                stmt.SetNamespace("test");

                // TODO: Set the name of the set on the instance of the Statement
                // Exercise A2
                stmt.SetSetName("users");

                // TODO: Set the name of index on the instance of the Statement
                // Exercise A2
                stmt.SetIndexName("tweetcount_index");

                // TODO: Set the list of bins to retrieve on the instance of the Statement
                // Exercise A2
                stmt.SetBinNames(bins);

                // TODO: Set the range Filter on tweetcount on the instance of the Statement
                // Exercise A2
                stmt.SetFilters(Filter.Range("tweetcount", min, max));

                Console.WriteLine("\nAggregating users with " + min + "-" + max + " tweets by region. Hang on...\n");

                // TODO: Execute the Aggregation Query passing null policy and Statement instance, 
                // Lua Module and module function to call.
                // Exercise A2
                rs = client.QueryAggregate(null, stmt, "aggregationByRegion", "sum");

                if (rs.Next())
                {
                    // TODO: Iterate through returned RecordSet and output text in format "Total Users in <region>: <#>"
                    // Exercise A2
                    Dictionary<object, object> result = (Dictionary<object, object>)rs.Object;
                    Console.WriteLine("Total Users in North: " + result["n"]);
                    Console.WriteLine("Total Users in South: " + result["s"]);
                    Console.WriteLine("Total Users in East: " + result["e"]);
                    Console.WriteLine("Total Users in West: " + result["w"]);
                }
            }
            finally
            {
                // TODO: Close the RecordSet
                // Exercise A2
                if (rs != null)
                {
                    // Close record set
                    rs.Close();
                }
            }
        } //aggregateUsersByTweetCountByRegion

        public void createUsers()
        {
            string[] genders = { "m", "f" };
            string[] regions = { "n", "s", "e", "w" };
            string[] randomInterests = {"Music","Football", "Soccer", "Baseball", "Basketball", "Hockey", "Weekend Warrior", "Hiking", "Camping", "Travel", "Photography"};
            string randomInterest;
            string username;
            List<object> userInterests = null;
            int totalInterests = 0;
            int start = 1;
            int end = 10000;
            int totalUsers = end - start + 1;
            Random rnd1 = new Random();
            Random rnd2 = new Random();
            Random rnd3 = new Random(); 

            WritePolicy wPolicy = new WritePolicy();
            wPolicy.recordExistsAction = RecordExistsAction.UPDATE;

            Console.WriteLine("\nCreate " + totalUsers + " users. Press any key to continue...");
            Console.ReadLine();

            for (int j = start; j <= end; j++)
            {
                // Write user record
                username = "user" + j;
                Key key = new Key("test", "users", username);
                Bin bin1 = new Bin("username", "user" + j);
                Bin bin2 = new Bin("password", "pwd" + j);
                Bin bin3 = new Bin("gender", genders[rnd1.Next(0, 2)]);
                Bin bin4 = new Bin("region", regions[rnd2.Next(0, 4)]);
                Bin bin5 = new Bin("lasttweeted", 0);
                Bin bin6 = new Bin("tweetcount", 0);

                totalInterests = rnd3.Next(1, 7);
                userInterests = new List<object>();
                for (int t = 0; t < totalInterests; t++)
                {
                    randomInterest = randomInterests[rnd3.Next(0, 9)];
                    userInterests.Add(randomInterest);
                }
                Bin bin7 = new Bin("interests", userInterests);

                client.Put(wPolicy, key, bin1, bin2, bin3, bin4, bin5, bin6, bin7);
                Console.WriteLine("Wrote user record for " + username);
            }

            Console.WriteLine("\nDone creating " + totalUsers + " users!");
        } //createUsers
    }
}
