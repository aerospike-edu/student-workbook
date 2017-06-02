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


//==========================================================
// Includes
//

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

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
void queryTweets(aerospike *as);
void aggregateUsersByTweetCountByRegion(aerospike *as);

int
main(int argc, char* argv[])
{
        int input;
	as_error err;
	as_config config;
	as_config_init(&config);

	as_config_add_hosts(&config, "127.0.0.1" , 3000);
	as_config_add_hosts(&config, "192.168.10.15" , 3000);

	aerospike as;
	aerospike_init(&as, &config);

        as_policy_write wpol;
        as_policy_write_init(&wpol);
        wpol.exists = AS_POLICY_EXISTS_IGNORE;

        if (aerospike_connect(&as, &err) != AEROSPIKE_OK) {
                LOG("aerospike_connect() returned %d - %s", err.code, err.message);
                aerospike_destroy(&as);
                exit(-1);
        }

	printf("\nWhat would you like to do:\n");
	printf("1> Create A User And A Tweet\n");
	printf("2> Read A User Record\n");
	printf("3> Batch Read Tweets For A User\n");
	printf("4> Scan All Tweets For All Users\n");
	printf("5> Record GEN -- Update User Password\n");
	printf("55> Record UDF -- Update User Password\n");
	printf("6> Query Tweets By Username And Users By Tweet Count Range\n");
	printf("7> Stream UDF -- Aggregation Based on Tweet Count By Region\n");
	printf("0> Exit\n");
	printf("\nSelect 0-7 and hit enter:\n");
	scanf("%d",&input);

	switch(input){
	  case 1:
	    printf("\n********** Your Selection: Create User And A Tweet **********\n");
	    createUser(&as,&wpol);
	    createTweet(&as,&wpol);
	    break;
	  case 2:
	    getUser(&as);
	    break;
	  case 3:
	    batchGetUserTweets(&as);
	    break;
	  case 4:
	    scanAllTweetsForAllUsers(&as);
	    break;
	  case 5:
	    updatePasswordUsingCAS(&as,&wpol);
	    break;
	  case 55:
	    updatePasswordUsingUDF(&as);
	    break;
	  case 6:
	    queryTweets(&as);
	    break;
	  case 7:
	    aggregateUsersByTweetCountByRegion(&as);
	    break;
	  case 12:
	    createUser(&as,&wpol);
	    break;
	  case 23:
	    createTweet(&as,&wpol);
	    break;
	  default:
	    break;
	}


	aerospike_close(&as,&err); 
	aerospike_destroy(&as);

}

