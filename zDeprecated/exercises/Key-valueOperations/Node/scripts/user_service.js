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

exports.createUser = function(client)	{

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
    if (answers.username !== "")  {

      // TODO: Create key and bin instances for the user record. Remember to convert comma-separated interests into a list array before storing it.
      // Exercise 2
      console.log("TODO: Create key and bin instances for the user record. Remember to convert comma-separated interests into a list array before storing it.");

      // TODO: Write user record
      // Exercise 2
      console.log("TODO: Write user record");

      // TODO: Write tweet record
      // Exercise 2
      tweet_service.createTweet(client);
    }

    console.log("INFO: User record NOT created!");

  });

};

exports.getUser = function(client) {

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
    var key = {
      ns:  "test",
      set: "users",
      key: answer.username
    };

    // TODO: Read user record
    // Exercise 2
    console.log("TODO: Read user record");

    // TODO: Output user record to the console
    // Exercise 2
    console.log("TODO: Output user record to the console");
  });

};

exports.updatePasswordUsingUDF = function(client)  {

  console.log("********** Update User Password **********");

};

exports.updatePasswordUsingCAS = function(client)  {

  console.log("********** Update User Password **********");

};

exports.batchGetUserTweets = function(client) {

  console.log("********** Batch Read Tweets For User **********");

  var question = [
    {
      type: "input",
      name: "username",
      message: "Enter username"
    }
  ];

  inquirer.prompt( question, function( answer ) {

    // TODO: Read user record
    // Exercise 3
    console.log("TODO: Read user record");

    // TODO: Get how many tweets the user has
    // Exercise 3
    console.log("TODO: Get how many tweets the user has");

    // TODO: Create an array of tweet keys -- keys[tweet_count]
    // Exercise 3
    console.log("TODO: Create an array of tweet keys -- keys[tweet_count]");

    // TODO: Initiate batchGet read operation
    // Exercise 3
    console.log("TODO: Initiate batchGet operation");

    // TODO: Output tweets to the console
    // Exercise 3
    console.log("TODO: Output tweets to the console");

  });

};

exports.createUsers = function(client)  {

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
  
  for (var i = start; i <= end; i++) {

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
    client.put(key, userRecord, function(err, rec, meta) {
      if ( err.code === 0 ) {
        // The record was successfully created.        
      } else {
         console.log("ERROR: createUsers failed: ", err);
      }
    });

    console.log("Wrote user record for " + username);
  }  

};

