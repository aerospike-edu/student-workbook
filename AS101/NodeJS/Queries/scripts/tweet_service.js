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

exports.createTweet = function(client, callback)	{

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
    // Exercise K2
    client.get(user_key, function(err, userrecord, meta) {
      // Check for errors
      if ( err == null ) {

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

          // Create Key and Bin instances for the tweet record.
          // Exercise K2
          var tweet_key = {
            ns:  "test",
            set: "tweets",
            key: userrecord.username + ":" + tweet_count
          };

          // Exercise K2
          var recBins = {
            username: userrecord.username,
            tweet: answer.tweet,
            ts: ts
          };

          // Exercise K2
          client.put(tweet_key, recBins, function(err, recKey) {
            // Check for errors
            // Exercise K2
            if ( err == null ) {
              console.log("INFO: Tweet record created!");

              // Update tweetcount and last tweet'd timestamp in the user record
              //updateUser(client, user_key, ts, tweet_count, callback);
              // Exercise K2  - add code to updateUser()
              updateUser(client, user_key, ts, tweet_count, callback);
            }
            else {
              console.log("ERROR: Tweet record not created!");
              console.log("",err);
              callback();
            }

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
function updateUser(client, user_key, ts, tweet_count, callback) {
  // Update User record
  // Create bins to update
  // Exercise K2
  var bins = {
    lasttweeted: ts,
    tweetcount: tweet_count
  };

  // Update the record
  // Exercise K2
  //Comment code section below for Exercise K6
  client.put(user_key, bins, function(err, recKey) {
    // Check for errors
    if ( err == null ) {
      console.log("INFO: User " + recKey['key'] +" record updated!");
      callback();
    }
    else {
      console.log("ERROR: User record not updated!");
      console.log(err);
      callback();
    }
  });
  // Exercise K6, uncomment line below
  // In Operate, we will use the increment operation, so don't need tweet_count
  //updateUserUsingOperate(client, user_key, ts, callback);
}

function updateUserUsingOperate(client, user_key, ts, callback) {
  // Update User record
  // User Operate() to set and get tweetcount
  // Exercise K6
  var operator = aerospike.operator;
  var operations = [operator.incr('tweetcount', 1),operator.write('lasttweeted', ts),operator.read('tweetcount')];
  client.operate(user_key, operations, function(err, record, metadata, key) {
    // Check for errors
    if ( err == null ) {
      console.log("INFO: (Operate) The tweet count now is: " + record.tweetcount);
    }
    else {
      console.log("ERROR: User record not updated!");
      console.log(err);
    }
    callback();
  });
}

exports.scanAllTweetsForAllUsers = function(client, callback)  {
  // Initiate scan operation that invokes callback for outputting tweets on the console
  // Exercise K4
  var query = client.query('test', 'tweets');
  var stream = query.execute();
  // Events returned by the stream are 'data', 'error' or 'end'.
  // Handle each event accordingly
  stream.on('data', function(record)  {
    // Handle data event.
    // Exercise K4
    console.log("("+ record['username'] +"):"+record['tweet']);
  });
  stream.on('error', function(err)  {
    // Handle error event.
    // Exercise K4
    console.log('ERROR: Scan All Tweets For All Users failed: ',err);
    callback();
  });
  stream.on('end', function()  {
    // Handle end event.
    // Exercise K4
    console.log('INFO: Scan All Tweets For All Users completed!');
    callback();
  });

};

exports.queryTweetsByUsername = function(client, callback)  {

  console.log("********** Query Tweets By Username **********");
  // NOTE: Index creation has been included in here for convenience and to demonstrate the syntax.
  // NOTE: The recommended way of creating indexes in production env is via AQL.
  // Create a Secondary Index on username
  // Exercise Q3
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

    // Check if User record exists
    client.get(user_key, function(err, userrecord, meta) {
      // Check for errors
      if ( err == null ) {
        // Create Query and Set equality Filter on username
        // Exercise Q3
        var statement = {filters:[aerospike.filter.equal('username', answer.username)]};
        var query = client.query('test', 'tweets', statement);
        // Execute the query
        // Exercise Q3
        var stream = query.foreach(null);  //Query Policy = null
        stream.on('data', function(record)  {
          // Handle 'data' event. Print Tweets for given Username
          // Exercise Q3
          console.log(record.tweet);
        });
        stream.on('error', function(err)  {
          // Handle 'error' event.
          // Exercise Q3
          console.log('ERROR: Query Tweets By Username failed: ',err);
          callback();
        });
        stream.on('end', function()  {
          // Handle 'end' event.
          // Exercise Q3
          console.log('INFO: Query Tweets By Username completed!');
          callback();
        });

      }
      else {
        console.log("ERROR: User record not found!");
        callback();
      }
    });

  });


};

exports.queryUsersByTweetCount = function(client, callback)  {

  console.log("********** Query Users By Tweet Count Range **********");
  // NOTE: Index creation has been included in here for convenience and to demonstrate the syntax.
  // NOTE: The recommended way of creating indexes in production env is via AQL.

  // Create a Secondary Index on tweetcount
  // Exercise Q4
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
    // Prepare query statement - Set range Filter on tweetcount
    // Exercise Q4
    var statement = {filters:[aerospike.filter.range('tweetcount', parseInt(answers.min), parseInt(answers.max))]};

    // Select bins of interest to retrieve from the query
    // Exercise Q4
    statement.select = ['username', 'tweetcount'];

    // Create query
    // Exercise Q4
    var query = client.query('test', 'users', statement);

    // Execute the query
    // Exercise Q4
    var stream = query.foreach(null);  //Query Policy = null

    // Handle 'data' event returned by the query
    // Exercise Q4
    stream.on('data', function(record)  {
      console.log(record.username + ' has ' + record.tweetcount +' tweets.');
    });

    // Handle 'error' event returned by the query
    // Exercise Q4
    stream.on('error', function(err)  {
      console.log('ERROR: Query Users By Tweet Count Range failed:\n',err);
      callback();
    });

    // Handle 'end' event returned by the query
    // Exercise Q4
    stream.on('end', function()  {
      console.log('INFO: Query Users By Tweet Count Range completed!');
      callback();
    });

  });
}

exports.aggregateUsersByTweetCountByRegion = function(client, callback)  {

  console.log("********** Aggregation Based on Tweet Count By Region **********");

    // NOTE: Index creation has been included in here for convenience and to demonstrate the syntax.
    // NOTE: The recommended way of creating indexes in production env is via AQL.

    // Create a Secondary Index on tweetcount
    // (Skip if you already have the index on Aerospike from Exercise Q4)
    // Exercise A2
    createIndexOnTweetcount(client);

    // Get min and max tweet counts for range filter
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

    //Note: answers.min and answers.max are string types
    inquirer.prompt( questions, function( answers ) {

      // NOTE: UDF registration has been included in here for convenience and to demonstrate the syntax.
      // NOTE: The recommended way of creating indexes in production env is via AQL.

      // Register UDF, if successful, prepare the aggregation query and execute it.
      // Exercise A2
      client.udfRegister('udf/aggregationByRegion.lua', function(err1) {
        if ( err1 == null ) {
          // Prepare query statement - Set range Filter on tweetcount
          // Exercise A2
          var statement = {filters:[aerospike.filter.range('tweetcount', parseInt(answers.min), parseInt(answers.max))]};

          // Create query
          // Exercise A2
          var query = client.query('test', 'users', statement);

          //Or you can also use the construct below using 'where' to create the query object:
          //var query = client.query('test', 'users');
          //query.where(aerospike.filter.range('tweetcount', parseInt(answers.min), parseInt(answers.max)));

          // Execute the query, invoking stream Aggregation UDF on the results of the query
          // UDF returns aggregated result
          // Exercise A2
          query.apply('aggregationByRegion', 'sum', function(err2, result)  {
            if (err2 == null) {
              // Display desired result: "Total Users In <region>: <#>"
              // Exercise A2
              console.log('Total Users In East:  ', result.e);
              console.log('Total Users In West:  ', result.w);
              console.log('Total Users In North: ', result.n);
              console.log('Total Users In South: ', result.s);
              callback();
            }
            else {
              console.log('ERROR: Aggregation Based on Tweet Count By Region failed: ',err2);
              callback();
            }
          });  //query.apply()
        }
        else {
          // An error occurred
          console.log('ERROR: aggregationByRegion UDF registeration failed: ', err1);
          callback();
        }
      });
    });
};

exports.createTweets = function(client, callback)  {

  console.log("********** Create Sample Tweets **********");
  //Pick 1000 users at random from the 10000 users we have and insert
  //0 to 20 (random) tweets for that users.  Expect Avg # = 10*1000 = 10,000 tweets.
  var start = 1;
  var end = 1000;
  var tweets = 0;
  var username;
  var randomTweets = ['For just $1 you get a half price download of half of the song. You will be able to listen to it just once.','People tell me my body looks like a melted candle','Come on movie! Make it start!','Byaaaayy','Please, please, win! Meow, meow, meow!','Put. A. Bird. On. It.','A weekend wasted is a weekend well spent','Would you like to super spike your meal?','We have a mean no-no-bring-bag up here on aisle two.','SEEK: See, Every, EVERY, Kind... of spot','We can order that for you. It will take a year to get there.','If you are pregnant, have a soda.','Hear that snap? Hear that clap?','Follow me and I may follow you','Which is the best cafe in Portland? Discuss...','Portland Coffee is for closers!','Lets get this party started!','How about them portland blazers!',"You got school'd, yo",'I love animals','I love my dog',"What's up Portland",'Which is the best cafe in Portland? Discuss...','I dont always tweet, but when I do it is on Tweetaspike'];

  //for (var i = start; i <= end; i++) {  //Upto 1000, will saturate default max 300 connections.
  var i = start;
  var writeTweetsForUser = ()=>{
    username = 'user'+Math.floor((Math.random() * 10000) + 1);

    // create tweets
    tweets = Math.floor((Math.random() * 20) + 1);
    var j = 1;
    var writeTweets = ()=>{ //Max upto 20 tweets for a user
      var tweetKey = aerospike.key('test','tweets',username+':'+j);
      // update tweet count

      client.put(tweetKey, {username: username, tweet: randomTweets[Math.floor((Math.random() * 10) + 1)], ts: randomTimestamp(new Date(2015, 0, 1), new Date())}, function(err, recKey) {
        if ( err == null ) {
          // tweet was successfully created
          console.log("Added tweet "+j+" for: "+username);
          updateTweetCount(client, username);
          j++;
          if (j <= tweets) {
            writeTweets();
          }
          else {
            i++;
            if(i <= end){
              writeTweetsForUser();
            }
            else {
              callback();
            }
          }
        }
        else {
          // An error occurred
          console.error('ERROR: createTweets failed:\n', err);
          callback();
        }

      });
    }
    console.log("Creating "+tweets+" tweets for "+username);
    writeTweets();
  }
  //}
  writeTweetsForUser();
};

///////////////////// HELPER FUNCTIONS



function updateTweetCount(client, username)  {  //fire and forget!
  var userKey = aerospike.key('test','users',username);
  var operator = aerospike.operator;
  var operations = [operator.incr('tweetcount', 1),operator.read('tweetcount')];
  client.operate(userKey, operations, function(err, record, metadata, key) {
    if ( err == null ) {
      console.log('INFO: Updated tweet count is: ' + record.tweetcount);
      //we don't need to tie anything to return of this callback. (fire and forget)
    }
    else  {
      console.log('ERROR: updateTweetCount failed:\n', err);
    }
  });
}

function randomTimestamp(start, end) {
  return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime())).toString();
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
    if ( err == null ) {
      console.log('INFO: createIndexOnUsername created!');
      //will show up asynchronously, we can ignore.
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
    if ( err == null ) {
      console.log('INFO: createIndexOnTweetcount created!');
      //will show up asynchronously, we can ignore.
    } else {
      // An error occurred
      console.log('ERROR: createIndexOnTweetcount failed:\n', err);
    }
  });
}
