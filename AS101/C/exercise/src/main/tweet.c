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
#include <time.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/aerospike_scan.h>
#include <aerospike/aerospike_udf.h>
#include <aerospike/as_bin.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_error.h>
#include <aerospike/as_config.h>
#include <aerospike/as_key.h>
#include <aerospike/as_query.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_password.h>
#include <aerospike/as_record.h>
#include <aerospike/as_record_iterator.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_val.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_batch.h>
#include <aerospike/as_scan.h>

#define LOG(_fmt, _args...) { printf(_fmt "\n", ## _args);fflush(stdout); }

void updateUser(aerospike *as,as_key *key,as_policy_write *wpol, int tc,long ts);
void updateUserUsingOperate(aerospike *as,as_key *key, long ts);
bool scan_cb(const as_val* p_val, void* udata);
bool scan_tweet_cb(const as_val* p_val, void* udata);
void queryTweetsByUsername(aerospike *as);
void queryUsersByTweetCount(aerospike *as);

void
createTweet(aerospike *as,as_policy_write *wpol){
        char username[256];
        char tweet[256];
        char tweetkey[512];
        long ts;
	as_error err;

	printf("\n********** Create Tweet **********\n");
	printf("Enter username: ");
        scanf("%s",username);
	if ( strlen(username) > 0 ){

// insert program
//  end insert program

	}


	return;
}

void
updateUser(aerospike *as,as_key *key,as_policy_write *wpol,int tc,long ts){

// insert program
//  end insert program

//		updateUserUsingOperate(as, key, ts);
}

void
updateUserUsingOperate(aerospike *as,as_key *key,long ts){
        char *username;
        char *password;
        char *gender;
        char *region;
        int tweetcount;
        long int lasttweeted;
	as_error err;

// insert program
//  end insert program

}

void
scanAllTweetsForAllUsers(aerospike *as){
        as_error err;


// insert program
//  end insert program

}

bool
scan_cb(const as_val* p_val, void* udata) {
	char * tweet;

        if (! p_val) {
                printf("scan callback returned null - scan is complete");
                return true;
        }
        as_record* p_rec = as_record_fromval(p_val);

        if (! p_rec) {
                printf("scan callback returned non-as_record object");
                return true;
        }

        tweet = as_record_get_str(p_rec,"tweet");
        printf("tweet:%s:\n",tweet);

        return true;

}

void
queryTweets(aerospike *as){
        queryTweetsByUsername(as);
        queryUsersByTweetCount(as);
}

void
queryTweetsByUsername(aerospike *as){
        printf("\n********** Query Tweets By Username **********\n");
        char username[256];
        as_error err;

        printf("Enter username: ");
        scanf("%s",username);
        if ( strlen(username) > 0 ){

// insert program
//  end insert program

	}
}

void
queryUsersByTweetCount(aerospike *as){
	int min;
	int max;
        as_error err;

        printf("\n********** Query Users By Tweet Count Range **********\n");

	printf("\nEnter Min Tweet Count:");
        scanf("%d",&min);
	printf("\nEnter Max Tweet Count:");
        scanf("%d",&max);

        if ( max >= min ){

// insert program
//  end insert program

	}
}

bool
scan_tweet_cb(const as_val* p_val, void* udata) {
	char * username;
	int tweetcount;

        if (! p_val) {
                printf("scan callback returned null - scan is complete");
                return true;
        }
        as_record* p_rec = as_record_fromval(p_val);

        if (! p_rec) {
                printf("scan callback returned non-as_record object");
                return true;
        }

        username = as_record_get_str(p_rec,"username");
        tweetcount = as_record_get_int64(p_rec,"tweetcount",INT64_MAX);
        printf("username:%s tweetcount:%d:\n",username,tweetcount);

        return true;

}

