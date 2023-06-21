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
var user_service = require('./user_service');

exports.createTweet = function(client)	{

  console.log("********** Create Tweet **********");

  ///*********************///
  ///*****Data Model*****///
  //Namespace: test
  //Set: tweets
      //Key: <username:<counter>>
      //Bins:
          //tweet - string 
          //ts - int (Stores epoch timestamp of the tweet)
          //username - string

      //Sample Key: dash:1
      //Sample Record:
          //{ tweet: 'Put. A. Bird. On. It.',
          //  ts: 1408574221,
          //  username: 'dash'
          //}
  ///*********************///

  var question = [
    {
      type: "input",
      name: "username",
      message: "Enter username"
    }
  ];

  inquirer.prompt( question, function( answer ) {

    var user_key = {
      ns:  "test",
      set: "users",
      key: answer.username
    };

    // Check if User record exists
    client.get(user_key, function(err, userrecord, meta) {
      // Check for errors
      if ( err.code === 0 ) {

        var tweet_count = userrecord.tweetcount + 1;
        var ts = new Date().getTime();
        
        var question = [
          {
            type: "input",
            name: "tweet",
            message: "Enter tweet"
          }
        ];

        inquirer.prompt( question, function( answer ) {

          // Write Tweet record
          var tweet_key = {
            ns:  "test",
            set: "tweets",
            key: userrecord.username + ":" + tweet_count
          };

          var bins = {
            username: userrecord.username,
            tweet: answer.tweet,
            ts: ts
          };

          client.put(tweet_key, bins, function(err, tweetrecord, meta) {
            // Check for errors
            if ( err.code === 0 ) {
              console.log("INFO: Tweet record created!");

              // Update tweetcount and last tweet'd timestamp in the user record
              updateUser(client, user_key, ts, tweet_count);
            }
            else {
              console.log("ERROR: Tweet record not created!");
              console.log("",err);
            }
          });

        });

      }
      else {
        console.log("ERROR: User record not found!");
      }
    });

  });

};

exports.scanAllTweetsForAllUsers = function(client)  {

  var query = client.query('test', 'tweets');
  var stream = query.execute();
  stream.on('data', function(record)  {
    console.log(record.bins.tweet);
  });
  stream.on('error', function(err)  {
    console.log('ERROR: Scan All Tweets For All Users failed: ',err);
  });
  stream.on('end', function()  {
    // console.log('INFO: Scan All Tweets For All Users completed!');
  });  

};

exports.queryTweetsByUsername = function(client)  {

  console.log("********** Query Tweets By Username **********");

  // NOTE: Index creation has been included in here for convenience and to demonstrate the syntax. 
  // NOTE: The recommended way of creating indexes in production env is via AQL.
  createIndexOnUsername(client);

  var question = [
    {
      type: "input",
      name: "username",
      message: "Enter username"
    }
  ];

  inquirer.prompt( question, function( answer ) {

    var user_key = {
      ns:  "test",
      set: "users",
      key: answer.username
    };

    // TODO: Read user record
    // Exercise 3
    console.log("TODO: Read user record");

    // TODO: Create an object containing aerospike filters to filter records based on username
    // Exercise 3
    console.log("TODO: Create an object containing aerospike filters to filter records based on username");

    // TODO: Create query instance passing in object created in step above
    // Exercise 3
    console.log("TODO: Create query instance passing in object created in step above");

    // TODO: Execute query
    // Exercise 3
    console.log("TODO: Execute query");

    // TODO: Add 'data' listener on query execution result object for outputting user tweets to the console
    // Exercise 3
    console.log("TODO: Add 'data' listener on query execution result object for outputting user tweets to the console");

    // TODO: Add 'error' listener on query execution result object for outputting error to the console
    // Exercise 3
    console.log("TODO: Add 'error' listener on query execution result object for outputting error to the console");

    // TODO: Add 'end' listener on on query execution result object for outputting operation complete message to the console
    // Exercise 3
    console.log("TODO: Add 'end' listener on query execution result object outputting operation complete message to the console");

    queryUsersByTweetCount(client);
  });

};

function queryUsersByTweetCount(client)  {

  console.log("********** Query Users By Tweet Count Range **********");

  // NOTE: Index creation has been included in here for convenience and to demonstrate the syntax. 
  // NOTE: The recommended way of creating indexes in production env is via AQL.
  createIndexOnTweetcount(client);

  var questions = [
    {
      type: "input",
      name: "min",
      message: "Enter Min Tweet Count"
    },
    {
      type: "input",
      name: "max",
      message: "Enter Max Tweet Count"
    }    
  ];

  inquirer.prompt( questions, function( answers ) {

    // TODO: Create an object containing aerospike filters to filter records based on min and max tweet count range and array of bins to retrieve. In this example, we want to output username and tweet count.
    // Exercise 4
    console.log("TODO: Create an object containing aerospike filters to filter records based on min and max tweet count range and array of bins to retrieve. In this example, we want to output username and tweet count.");

    // TODO: Create query instance passing in object created in step above 
    // Exercise 4
    console.log("TODO: Create query instance passing in object created in step above");

    // TODO: Execute query
    // Exercise 4
    console.log("TODO: Execute query");

    // TODO: Add 'data' listener on query execution result object for outputting which user has how many tweets to the console
    // Exercise 4
    console.log("TODO: Add 'data' listener on query execution result object for outputting which user has how many tweets to the console");

    // TODO: Add 'error' listener on query execution result object for outputting error to the console
    // Exercise 4
    console.log("TODO: Add 'error' listener on query execution result object for outputting error to the console");

    // TODO: Add 'end' listener on on query execution result object for outputting operation complete message to the console
    // Exercise 4
    console.log("TODO: Add 'end' listener on query execution result object outputting operation complete message to the console");

  });

}

exports.aggregateUsersByTweetCountByRegion = function(client)  {

  console.log("********** Aggregation Based on Tweet Count By Region **********");

};

exports.createTweets = function(client)  {

  console.log("********** Create Sample Tweets **********");

  var start = Math.floor((Math.random() * 1) + 1);
  var end = Math.floor((Math.random() * 5000) + 1);
  var tweets = 0;
  var username;
  var randomTweets = ['For just $1 you get a half price download of half of the song. You will be able to listen to it just once.','People tell me my body looks like a melted candle','Come on movie! Make it start!','Byaaaayy','Please, please, win! Meow, meow, meow!','Put. A. Bird. On. It.','A weekend wasted is a weekend well spent','Would you like to super spike your meal?','We have a mean no-no-bring-bag up here on aisle two.','SEEK: See, Every, EVERY, Kind... of spot','We can order that for you. It will take a year to get there.','If you are pregnant, have a soda.','Hear that snap? Hear that clap?','Follow me and I may follow you','Which is the best cafe in Portland? Discuss...','Portland Coffee is for closers!','Lets get this party started!','How about them portland blazers!',"You got school'd, yo",'I love animals','I love my dog',"What's up Portland",'Which is the best cafe in Portland? Discuss...','I dont always tweet, but when I do it is on Tweetaspike'];

  for (var i = start; i <= end; i++) {
    username = 'user'+Math.floor((Math.random() * 10000) + 1);

    // create tweets
    tweets = Math.floor((Math.random() * 10) + 1);
    console.error("Created "+tweets+" tweets for "+username);
    for (var j = 1; j <= tweets; j++) {
      var tweetKey = aerospike.key('test','tweets',username+':'+j);
      // update tweet count
      updateTweetCount(client, username);
      client.put(tweetKey, {username: username, tweet: randomTweets[Math.floor((Math.random() * 10) + 1)], ts: randomTimestamp(new Date(2015, 0, 1), new Date())}, function(err, rec, meta) {
        if ( err.code === 0 ) {
          // tweet was successfully created          
        } else {
          // An error occurred
          console.error('ERROR: createTweets failed:\n', err);
        }
      });

      for (var k=0;k<2000000;k++){                    
        // dummy sleep. NOT FOR PROD USE.
      }

    }
  }

};

///////////////////// HELPER FUNCTIONS
function updateUser(client, user_key, ts, tweet_count) {
  // Update User record
  var bins = {
    lasttweeted: ts,
    tweetcount: tweet_count
  };

  client.put(user_key, bins, function(err, rec, meta) {
    // Check for errors
    if ( err.code === 0 ) {
      // console.log("INFO: User record updated!");
    }
    else {
      console.log("ERROR: User record not updated!");
      console.log(err);
    }
  });
}

function updateUserUsingOperate(client, user_key, ts) {
  // Update User record
  var operator = aerospike.operator;
  var operations = [operator.incr('tweetcount', 1),operator.write('lasttweeted', ts),operator.read('tweetcount')];
  client.operate(user_key, operations, function(err, bins, metadata, key) {
    // Check for errors
    if ( err.code === 0 ) {
      console.log("INFO: The tweet count now is: " + bins.tweetcount);
    }
    else {
      console.log("ERROR: User record not updated!");
      console.log(err);
    }
  });
}

function createIndexOnUsername(client)  {
  // NOTE: Index creation has been included in here for convenience and to demonstrate the syntax. 
  // NOTE: The recommended way of creating indexes in production env is via AQL.
  var options = {
    ns:  'test',
    set: 'tweets',
    bin : 'username',
    index: 'username_index'
  };
  client.createStringIndex(options, function(err)  {
    if ( err.code === 0 ) {     
      // console.log('INFO: createIndexOnUsername created!');
    } else {
      // An error occurred
      console.log('ERROR: createIndexOnUsername failed:\n', err);
    }
  });
}

function createIndexOnTweetcount(client)  {
  // NOTE: Index creation has been included in here for convenience and to demonstrate the syntax. 
  // NOTE: The recommended way of creating indexes in production env is via AQL.
  var options = {
    ns:  'test',
    set: 'users',
    bin : 'tweetcount',
    index: 'tweetcount_index'
  };
  client.createIntegerIndex(options, function(err)  {
    if ( err.code === 0 ) {     
      // console.log('INFO: createIndexOnTweetcount created!');
    } else {
      // An error occurred
      console.log('ERROR: createIndexOnTweetcount failed:\n', err);
    }
  });
}

function updateTweetCount(client, username)  {
  var userKey = aerospike.key('test','users',username); 
  var operator = aerospike.operator;
  var operations = [operator.incr('tweetcount', 1),operator.read('tweetcount')];
  client.operate(userKey, operations, function(err, bins, metadata, key) {
    if ( err.code === 0 ) {
      // console.log('INFO: The tweet count now is: ' + bins.tweetcount);
    }
    else  {
      console.log('ERROR: updateTweetCount failed:\n', err);
    }
  });  
}

function randomTimestamp(start, end) {
  return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime())).toString();
}


