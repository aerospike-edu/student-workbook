//  *******************************************************************************
//  * Copyright 2012-2015 by Aerospike.
//  *
//  * Permission is hereby granted, free of charge, to any person obtaining a copy
//  * of this software and associated documentation files (the "Software"), to
//  * deal in the Software without restriction, including without limitation the
//  * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
//  * sell copies of the Software, and to permit persons to whom the Software is
//  * furnished to do so, subject to the following conditions:
//  *
//  * The above copyright notice and this permission notice shall be included in
//  * all copies or substantial portions of the Software.
//  *
//  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
//  * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
//  * IN THE SOFTWARE.

'use strict';

var aerospike = require('aerospike');
var inquirer = require('inquirer');
var tweet_service = require('./tweet_service');

exports.createUser = function(client, callback)	{

  console.log("********** Create User **********");

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

  var questions = [
    {
      type: "input",
      name: "username",
      message: "Enter username"
    },
    {
      type: "input",
      name: "password",
      message: "Enter password"
    },
    {
      type: "input",
      name: "gender",
      message: "Select gender (f or m)",
      validate: function( value ) {
        if (value.toLowerCase() === 'f' || value.toLowerCase() === 'm') {
          return true;
        } else {
          return "Please enter either f or m";
        }
      }
    },
    {
      type: "input",
      name: "region",
      message: "Select region (n, s, e or w)",
      validate: function( value ) {
        if (value.toLowerCase() === 'n' || value.toLowerCase() === 's' || value.toLowerCase() === 'e' || value.toLowerCase() === 'w') {
          return true;
        } else {
          return "Please enter either n, s, e or w";
        }
      }
    },
    {
      type: "input",
      name: "interests",
      message: "Enter comma-separated interests"
    }
  ];

  inquirer.prompt( questions, function( answers ) {

    // Write User record
    // Exercise K2
    if (answers.username !== "")  {

      // Create Key object
      // Exercise K2
      var key = {
        ns:  "test",
        set: "users",
        key: answers.username
      };

      // Create record bins, convert comma-separated interests to List
      // Exercise K2
      var recBins = {
        username: answers.username,
        password: answers.password,
        gender: answers.gender,
        region: answers.region,
        lasttweeted: 0,
        tweetcount: 0,
        interests: answers.interests.split(",")
      };

      // Write the user record
      // Exercise K2
      client.put(key, recBins, function(err, recKey) {
        // Check for errors
        // Exercise K2
        if ( err == null ) {
          console.log("INFO: User "+ recKey['key'] + " record created!");
        }
        else {
          console.log("ERROR: createUser(): User record not created!");
          console.log(err);
        }
        callback();
      });
    } else{
      callback();
    }

  });

};

exports.getUser = function(client, callback) {

  console.log("********** Read User Record **********");

  var question = [
    {
      type: "input",
      name: "username",
      message: "Enter username"
    }
  ];

  inquirer.prompt( question, function( answer ) {

    // Read User record
    // Create user key
    // Exercise K2
    var key = {
      ns:  "test",
      set: "users",
      key: answer.username
    };

    client.get(key, function(err, rec, meta) {
      // Check for errors (ie does the user record exist?)
      // Exercise K2
      if ( err == null ) {
        // Print user info to console in: "<bin>" : value format
        // Exercise K2
        console.log("INFO: User record read successfully! Here are the details:");
        console.log("username:   " + rec.username);
        console.log("password:   " + rec.password);
        console.log("gender:     " + rec.gender);
        console.log("region:     " + rec.region);
        console.log("tweetcount: " + rec.tweetcount);
        console.log("lasttweeted: " + rec.lasttweeted);
        console.log("interests:  " + rec.interests);
      }
      else {
        console.log("ERROR: User record not found!");
      }
      callback();
    });
  });
};

exports.updatePasswordUsingUDF = function(client, callback)  {

  console.log("********** Update User Password Using UDF**********");
  var questions = [
    {
      type: "input",
      name: "username",
      message: "Enter username"
    },
    {
      type: "input",
      name: "password",
      message: "Enter password"
    }
  ];

  inquirer.prompt( questions, function( answers ) {

    // Read User record
    // Create user key
    var key = {
      ns:  "test",
      set: "users",
      key: answers.username
    };

    // Read user record
    client.get(key, function(err, rec, meta) {
      // Check for errors, is this a valid user record?
      // Exercise R2
      if ( err == null ) {
        // NOTE: UDF registration has been included in here for convenience and to demonstrate the syntax.

        // Register UDF
        // Exercise R2
        //TODO ...client.udfRegister(....
          if ( err == null ) {
            // Create UDF object for record udf execution
            // Exercise R2
            ////TODO ...var UDF =

            // We already created the user key
            // Execute Record UDF
            // Exercise R2
            //TODO ...client.execute(...
              // Check for errors
              //if ( err == null ) {
                //TODO ...
                // Print updated password
                // Exercise R2
                //TODO ...client.get(
                //TODO ...
                //});
              //}
              //else {
              //  console.log("ERROR: User password update failed\n", err);
              //  callback();
              //}
            //});
          } else {
            // An error occurred
            console.error("ERROR: updateUserPwd UDF registeration failed:\n", err);
            callback();
          }
        });
      }
      else {
        console.log("ERROR: User record not found!");
        callback();
      }
    });

  });
};

exports.updatePasswordUsingCAS = function(client, callback)  {

  console.log("********** Update User Password Using CAS**********");
  var question1 = [
    {
      type: "input",
      name: "username",
      message: "Enter username"
    }
  ];

  inquirer.prompt( question1, function( answer1 ) {

    // Read User record
    // Create user key
    var key = {
      ns:  "test",
      set: "users",
      key: answer1.username
    };

    client.get(key, function(err, rec, meta) {
      // Check for errors
      if ( err == null ) {
        // Current record generation is in meta.gen.
        //console.log("INFO: Current User record generation count: ", meta.gen);

        // Get new password
        var question2 = [
          {
            type: "input",
            name: "password",
            message: "Enter password"
          }
        ];

        inquirer.prompt( question2, function( answer2 ) {

         // Note: When running this test,
         // before entering new password on the prompt,
         // if you change the password using
         // another client such as AQL, gen EQ test will fail
         // and CAS failure can be verified. (Extra Credit!)

         // Set the generation count to the current one from the user record.
         // Then, setting writePolicy.gen to aerospike.policy.gen.EQ will ensure
         // we don't have a 'dirty-read' when updating user's password

         // Set generation value to what we just read
         // Exercise K5
          var metadata = {
            gen: meta.gen
          };

          // Set write Policy parameters
          // Set write policy for gen to be aerospike.policy.gen.EQ
          // Exercise K5
          var writePolicy = {
            gen : aerospike.policy.gen.EQ,
            key : aerospike.policy.key.SEND,
            retry : aerospike.policy.retry.NONE,
            exists : aerospike.policy.exists.IGNORE,
            commitLevel : aerospike.policy.commitLevel.ALL
          };

          // Set new password in password bin for the user record
          // Exercise K5
          var recordObj = {
            password: answer2.password
          };

          // Write the record update. Handle errors.
          // Exercise K5
          client.put(key, recordObj, metadata, writePolicy, function(err, recKey) {
            // Check for errors
            if ( err == null ) {
              console.log("INFO: User password updated successfully!");
            }
            else {
              console.log("ERROR: User password update failed:\n", err);
            }
            callback();
          });

        });

      }
      else {
        console.log("ERROR: User record not found!");
        callback();
      }
    });

  });
};

exports.batchGetUserTweets = function(client, callback) {

  console.log("********** Batch Read Tweets For User **********");

  var question = [
    {
      type: "input",
      name: "username",
      message: "Enter username"
    }
  ];

  inquirer.prompt( question, function( answer ) {

    // Read User record
    // Create user key
    var key = {
      ns:  "test",
      set: "users",
      key: answer.username
    };

    // Read user record
    // Exercise K3
    client.get(key, function(err, userrecord, meta) {
      // Check for errors
      if ( err == null ) {

        // Using total tweets by user, create a set of tweet keys.
        // Note: tweet key string is in the format <username>:<#> where # is 1 through user tweetcount

        // Get user tweet count.
        // Exercise K3
        var tweet_count = userrecord.tweetcount;

        // Create a list object to hold tweet key objects.
        var tweet_keys = [];

        // Create list of tweet key objects for tweets to retrieve
        // Exercise K3
        for(var i=1;i<=tweet_count;i++)  {
          tweet_keys.push({ns: "test", set: "tweets", key: answer.username + ":" + i});
        }

        // Batch read tweets
        // Exercise K3
        client.batchGet(tweet_keys, function (err, results) {
          // Check for errors
          if ( err == null ) {
            // Print out the tweets retreived
            // Exercise K3
            for(var j=0;j<results.length;j++)  {
              console.log(results[j].record.tweet);
            }
          }
          else {
            console.log("ERROR: Batch Read Tweets For User failed\n", err);
          }
          callback();  // Batch read returns all records together
        });

      }
      else {
        console.log("ERROR: User record not found!");
        callback();
      }
    });

  });

};

exports.createUsers = function(client, callback)  {

  console.log("********** Create Sample Users **********");

  var start = 1;
  var end = 10000;
  var key;
  var username;
  var userRecord;
  var record;
  var password;
  var genders = ['m','f','m','f','m','f','m','f','m','f','m','f'];
  var regions = ['e','w','s','n','e','w','s','n','e','w','s','n'];
  var randomInterests = ["Music","Football", "Soccer", "Baseball", "Basketball", "Hockey", "Weekend Warrior", "Hiking", "Camping", "Travel", "Photography"];

  //for (var i = start; i <= end; i++) {  //<==can't do this. You will saturate max connections!
  var i = start;
  var writeRec = ()=>{   //Using Arrow function notation
    username = 'user'+i;
    password = 'pwd'+i;
    var gender = genders[Math.floor((Math.random() * 10) + 1)]
    var region = regions[Math.floor((Math.random() * 10) + 1)]
    var interests = [];
    interests.push(randomInterests[Math.floor((Math.random() * 10) + 1)]);
    interests.push(randomInterests[Math.floor((Math.random() * 10) + 1)]);
    interests.push(randomInterests[Math.floor((Math.random() * 10) + 1)]);

    userRecord = {username: username, password: password, gender: gender, region: region, tweetcount: 0, lasttweeted: 0, interests: interests};

    key = aerospike.key('test','users',username);
    client.put(key, userRecord, (err, recKey)=>{
      if ( err == null ) {
        // The record was successfully created.
        console.log("Wrote user record for " + recKey['key']);
        i++;
        if(i <= end){
          writeRec();  //recurse to write next record
        }
        else{
          callback();  //done
        }

      } else {
         console.log("ERROR: createUsers failed: ", err);
         callback();
      }

    });
  }
  writeRec();  //Calls itself recursively till done in put() callback. ie writes one record at a time.
  //}

};
