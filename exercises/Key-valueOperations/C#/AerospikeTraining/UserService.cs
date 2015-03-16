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
                // Exercise 2
                Console.WriteLine("\nTODO: Create WritePolicy instance");

                // TODO: Create Key and Bin instances for the user record. Remember to convert comma-separated interests into a list before storing it.
                // Exercise 2
                Console.WriteLine("\nTODO: Create Key and Bin instances for the user record. Remember to convert comma-separated interests into a list before storing it.");

                // TODO: Write user record
                // Exercise 2
                Console.WriteLine("\nTODO: Write user record");

                Console.WriteLine("\nINFO: User record NOT created!");
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
                // TODO: Read user record
                // Exercise 2
                Console.WriteLine("\nTODO: Read user record");
                if (userRecord != null)
                {
                    // TODO: Output user record to the console. Remember to convert comma-separated interests into a list before outputting it.
                    // Exercise 2
                    Console.WriteLine("\nTODO: Output user record to the console. Remember to convert comma-separated interests into a list before outputting it.");
                }
                else
                {
                    Console.WriteLine("ERROR: User record not found!");
                }
            }
            else
            {
                Console.WriteLine("ERROR: User record not found!");
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
                // TODO: Read user record
                // Exercise 3
                Console.WriteLine("\nTODO: Read user record");
                if (userRecord != null)
                {
                    // TODO: Get how many tweets the user has
                    // Exercise 3
                    Console.WriteLine("\nTODO: Get how many tweets the user has");

                    // TODO: Create an array of tweet keys -- keys[tweetCount]
                    // Exercise 3
                    Console.WriteLine("\nTODO: Create an array of tweet keys -- keys[tweetCount]");

                    // TODO: Initiate batch read operation
                    // Exercise 3
                    Console.WriteLine("\nTODO: Initiate batch read operation");

                    // TODO: Output tweets to the console
                    // Exercise 3
                    Console.WriteLine("\nTODO: Output tweets to the console");
                }
                else
                {
                    Console.WriteLine("ERROR: User record not found!");
                }
            }
            else
            {
                Console.WriteLine("ERROR: User record not found!");
            }
        } //getUserTweets

        public void updatePasswordUsingUDF()
        {
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
                // Check if username exists
                userKey = new Key("test", "users", username);
                userRecord = client.Get(null, userKey);
                if (userRecord != null)
                {
                    // Get new password
                    string password;
                    Console.WriteLine("Enter new password for " + username + ":");
                    password = Console.ReadLine();

                    // TODO: Update User record with new password only if generation is the same
                    // Exercise 5
                }
                else
                {
                    Console.WriteLine("ERROR: User record not found!");
                }
            }
            else
            {
                Console.WriteLine("ERROR: User record not found!");
            }
        } //updatePasswordUsingCAS

        public void aggregateUsersByTweetCountByRegion()
        {
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
            int start = 0;
            int end = 100000;
            int totalUsers = end - start;
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
                Bin bin7 = Bin.AsList("interests", userInterests);

                client.Put(wPolicy, key, bin1, bin2, bin3, bin4, bin5, bin6, bin7);
                Console.WriteLine("Wrote user record for " + username);
            }

            Console.WriteLine("\nDone creating " + totalUsers + "!");
        } //createUsers
    }
}
