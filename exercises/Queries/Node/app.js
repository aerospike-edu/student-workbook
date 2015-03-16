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
var user_service = require('./scripts/user_service');
var tweet_service = require('./scripts/tweet_service');

// Connect to the Aerospike Cluster
var client = aerospike.client({
    hosts: [ { addr: '172.16.159.170', port: 3000 } ]
}).connect(function(response) {
    // Check for errors
    if ( response.code == aerospike.status.AEROSPIKE_OK ) {
      // Connection succeeded
      console.log("Connection to the Aerospike cluster succeeded!");
    }
    else {
      // Connection failed
      console.log("Connection to the Aerospike cluster failed. Please check cluster IP and Port settings and try again.");
      process.exit(0);
    }
});

// Setup tear down
process.on('exit', function() {
  if (client != null) {
    client.close();
    // console.log("Connection to Aerospike cluster closed!");
  }
});

// Present Menu
inquirer.prompt([
  {
    type: "rawlist",
    name: "answer",
    message: "What would you like to do:",
    choices: [
      "Create User And Tweet",
      "Read User Record",
      "Batch Read Tweets For User",
      "Scan All Tweets For All Users",
      "Record UDF -- Update User Password",
      "Query Tweets By Username And Users By Tweet Count Range",
      "Stream UDF -- Aggregation Based on Tweet Count By Region",
      new inquirer.Separator(),
      "Create Sample Users And Tweets"
    ]
  }
], function( answers ) {
  // console.log( answers.answer );

  switch (answers.answer) {
    case "Create User And Tweet":
      user_service.createUser(client);      
      break;
    case "Read User Record":
      user_service.getUser(client);
      break;
    case "Batch Read Tweets For User":
      user_service.batchGetUserTweets(client);
      break;
    case "Scan All Tweets For All Users":
      tweet_service.scanAllTweetsForAllUsers(client);
      break;
    case "Record UDF -- Update User Password":
      user_service.updatePasswordUsingUDF(client);
      break;
    case "Query Tweets By Username And Users By Tweet Count Range":
      tweet_service.queryTweetsByUsername(client);
      break;
    case "Stream UDF -- Aggregation Based on Tweet Count By Region":
      tweet_service.aggregateUsersByTweetCountByRegion(client);
      break;
    case "Create Sample Users And Tweets":

      inquirer.prompt([
        {
          type: "rawlist",
          name: "answer",
          message: "What would you like to do:",
          choices: [
            "Create Sample Users",
            "Create Sample Tweets"
          ]
        }
      ], function( answers ) {

        switch (answers.answer) {
          case "Create Sample Users":
            user_service.createUsers(client);      
            break;
          case "Create Sample Tweets":
            tweet_service.createTweets(client);
            break;
          default:
            break;
        }
      });
      
      break;
    default:
      process.exit(0);
      break;
  }
});
