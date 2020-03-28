/*******************************************************************************
 * Copyright 2008-2016 by Aerospike.
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

// Original author: Tak Nakadai
// email: nakadai@aerospike.com

//==========================================================
// Includes
//

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>

#define LOG(_fmt, _args...) { printf(_fmt "\n", ## _args);fflush(stdout); }

void createUser(aerospike* as,as_policy_write* wpol);
void getUser(aerospike* as);
void updatePasswordUsingCAS(aerospike* as,as_policy_write* wpol);
void updatePasswordUsingUDF(aerospike* as);
void batchGetUserTweets(aerospike* as);
void createTweet(aerospike* as,as_policy_write* wpol);
void scanAllTweetsForAllUsers(aerospike* as);
void queryTweetsByUsername(aerospike *as);
void queryUsersByTweetCount(aerospike *as);
void aggregateUsersByTweetCountByRegion(aerospike *as);

int
main(int argc, char* argv[])
{
	int input;

	as_error err;
	as_config config;


	//Exercise K1
	as_config_init(&config);
	as_config_add_hosts(&config, "127.0.0.1" , 3000);
	//as_config_add_hosts(&config, "192.168.10.15" , 3000);
	//strcpy(config.lua.user_path, "/home/pgupta/trainingSrcRepo/student-workbook/AS101/C/solution/udf");

	//Exercise R2, Exercise A2
	/*//Add Code ...
	strcpy(config.lua.user_path, "...");
	//manually copy client side stream udf to this location

	//strcpy(config.lua.system_path, "...");  //deprecated field
	*/

	aerospike as;
	aerospike_init(&as, &config);

	as_policy_write wpol;
	as_policy_write_init(&wpol);
	wpol.exists = AS_POLICY_EXISTS_IGNORE;

	//Exercise K1
	if (aerospike_connect(&as, &err) != AEROSPIKE_OK) {
		LOG("aerospike_connect() returned %d - %s", err.code, err.message);
		aerospike_destroy(&as);
		exit(-1);
	}

	printf("\nWhat would you like to do:\n");
	printf("1> Create A User\n");
	printf("2> Create A Tweet By A User\n");
	printf("3> Read A User Record\n");
	printf("4> Batch Read Tweets For A User\n");
	printf("5> Scan All Tweets For All Users\n");
	printf("6> Update User Password using CAS\n");
	printf("7> Update User Password using Record UDF\n");
	printf("8> Query Tweets By Username\n");
	printf("9> Query Users By Tweet Count Range\n");
	printf("10> Stream UDF -- Aggregation Based on Tweet Count By Region\n");
	//printf("11> Create a Test Set of Users\n");
	//printf("12> Create a Test Set of Tweets\n");
	printf("0> Exit\n");
	printf("\nSelect 0-10 and hit enter:\n");
	scanf("%d",&input);


	switch(input){
	case 1:
		printf("\n********** Your Selection: Create A User **********\n");
		createUser(&as,&wpol);
		break;
	case 2:
		printf("\n********** Your Selection: Create A Tweet By A User **********\n");
		createTweet(&as,&wpol);
		break;
	case 3:
		printf("\n********** Your Selection: Read A User Record **********\n");
		getUser(&as);
		break;
	case 4:
		printf("\n********** Your Selection: Batch Read Tweets For A User **********\n");
		batchGetUserTweets(&as);
		break;
	case 5:
		printf("\n********** Your Selection: Scan All Tweets For All Users **********\n");
		scanAllTweetsForAllUsers(&as);
		break;
	case 6:
		printf("\n********** Your Selection: Update User Password using CAS **********\n");
		updatePasswordUsingCAS(&as,&wpol);
		break;
	case 7:
		printf("\n********** Your Selection: Update User Password using Record UDF **********\n");
		updatePasswordUsingUDF(&as);
		break;
	case 8:
		printf("\n********** Your Selection: Query Tweets By Username **********\n");
		queryTweetsByUsername(&as);
		break;
	case 9:
		printf("\n********** Your Selection: Query Users By Tweet Count Range  **********\n");
		queryUsersByTweetCount(&as);
		break;
	case 10:
		printf("\n********** Your Selection: Stream UDF -- Aggregation Based on Tweet Count By Region **********\n");
		aggregateUsersByTweetCountByRegion(&as);
		break;
		/*
	  case 11:
		  printf("\n********** Your Selection: Create a Test Set of Users **********\n");
		  createUsersTestSet(&as,&wpol);
		  break;
	  case 12:
		  printf("\n********** Your Selection: Create a Test Set of Tweets **********\n");
		  createTweetsTestSet(&as,&wpol);
		  break;
		 */
	default:
		break;
	}

    //Exercise K1
	aerospike_close(&as,&err); 
	aerospike_destroy(&as);

}

