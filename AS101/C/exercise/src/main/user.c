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
#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_query.h>
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

#define LOG(_fmt, _args...) { printf(_fmt "\n", ## _args);fflush(stdout); }

bool batch_read_cb(const as_batch_read* results, uint32_t n, void* udata);
bool register_udf(aerospike* p_as, const char* udf_file_path);
bool query_cb_map(const as_val* p_val, void* udata);
int split( char *str, const char *delim, char *outlist[] );

void
createUser(aerospike *as,as_policy_write* wpol){
        char username[256];
        char password[256];
        char gender[256];
        char region[256];
        char interests[1256];
        char *interestslist[256];
	int i,cnt;
	as_error err;

	printf("Enter username: ");
        scanf("%s",username);
	if ( strlen(username) > 0 ){
		printf("Enter password for %s:",username);
	        scanf("%s",password);
		printf("Select gender (f or m) for %s:",username);
	        scanf("%s",gender);
                gender[1] = '\0';
		printf("Select region (north, south, east or west) for %s;",username);
	        scanf("%s",region);
                region[1] = '\0';
		printf("Enter comma-separated interests for %s:",username);
	        scanf("%s",interests);

// insert program
// end insert program
		printf("\nINFO: User record created!");
	}


	return;
}

void
getUser(aerospike *as){
        char username[256];
        char *password;
        char *gender;
        char *region;
	int tweetcount;
	as_list *interestlist;
	as_error err;

	printf("Enter username: ");
        scanf("%s",username);
	if ( strlen(username) > 0 ){

// insert program
// end insert program


	}
}

void
batchGetUserTweets(aerospike *as){
        char username[256];
        char tweetkey[256];
        char *tweetkeyp;
	as_error err;

	printf("Enter username: ");
        scanf("%s",username);
	if ( strlen(username) > 0 ){

// insert program
// end insert program

	}

}

bool
batch_read_cb(const as_batch_read* results, uint32_t n, void* udata)
{
	char * tweet;

        LOG("batch read callback returned %u record results:", n);

        for (uint32_t i = 0; i < n; i++) {
		tweet = as_record_get_str((as_record *)&results[i].record,"tweet");
                printf("index %u, key %s: tweet :%s\n", i,
                                as_string_getorelse((as_string*)results[i].key->valuep, "ERR"),tweet);
	}
	return true;
}

void
updatePasswordUsingCAS(aerospike* as,as_policy_write* wpol){
        char username[256];
        char password[256];
	as_error err;

	printf("Enter username: ");
        scanf("%s",username);
	if ( strlen(username) > 0 ){

// insert program
// end insert program

	}
}

void
updatePasswordUsingUDF(aerospike* as){
        char username[256];
        char password[256];
        as_error err;

        printf("Enter username: ");
        scanf("%s",username);
        if ( strlen(username) > 0 ){

// insert program
// end insert program

        }


}

void aggregateUsersByTweetCountByRegion(aerospike *as){
       int min;
        int max;
        as_error err;

        printf("\n********** Query Users By Tweet Count Range **********\n");

        printf("\nEnter Min Tweet Count:");
        scanf("%d",&min);
        printf("\nEnter Max Tweet Count:");
        scanf("%d",&max);

        if ( max >= min ){
               if(!register_udf(as, "udf/aggregationByRegion.lua")){
                         LOG("UDF registration error.");
                         return;
               }
	        as_query query;
        	as_query_init(&query, "test", "users");
	        as_query_where_inita(&query, 1);
       		as_query_where(&query, "tweetcount", as_integer_range(min, max));

	        as_query_apply(&query, "aggregationByRegion", "sum", NULL);
                if (aerospike_query_foreach(as, &err, NULL, &query, query_cb_map, NULL) !=
                        AEROSPIKE_OK) {
                	LOG("aerospike_query_foreach() returned %d - %s", err.code,
                                err.message);
                	as_query_destroy(&query);
                	exit(-1);
        	}

        	LOG("map-reduce query executed");
		as_query_destroy(&query);
	}
}


bool
register_udf(aerospike* p_as, const char* udf_file_path)
{
        FILE* file = fopen(udf_file_path, "r");

        if (! file) {
                LOG("cannot open script file %s ", udf_file_path);
                return false;
        }

        uint8_t* content = (uint8_t*)malloc(1024 * 1024);

        if (! content) {
                LOG("script content allocation failed");
                fclose(file);
                return false;
        }

        uint8_t* p_write = content;
        int read = (int)fread(p_write, 1, 512, file);
        int size = 0;

        while (read) {
                size += read;
                p_write += read;
                read = (int)fread(p_write, 1, 512, file);
        }

        fclose(file);

        as_bytes udf_content;
        as_bytes_init_wrap(&udf_content, content, size, true);

        as_error err;
        as_string base_string;
        const char* base = as_basename(&base_string, udf_file_path);

        if (aerospike_udf_put(p_as, &err, NULL, base, AS_UDF_TYPE_LUA,
                        &udf_content) == AEROSPIKE_OK) {
                aerospike_udf_put_wait(p_as, &err, NULL, base, 100);
        }
        else {
                LOG("aerospike_udf_put() returned %d - %s", err.code, err.message);
        }

        as_string_destroy(&base_string);
        as_bytes_destroy(&udf_content);

        return err.code == AEROSPIKE_OK;
}

int split( char *str, const char *delim, char *outlist[] ) {
    char    *tk;
    int     cnt = 0;

    tk = strtok( str, delim );
    while( tk != NULL && cnt < 256 ) {
        outlist[cnt++] = tk;
        tk = strtok( NULL, delim );
    }
    return cnt;
}

bool
query_cb_map(const as_val* p_val, void* udata) {
        if (! p_val) {
                LOG("query callback returned null - query is complete");
                return true;
        }
        if (! as_map_fromval(p_val)) {
                LOG("query callback returned non-as_map object");
                return true;
        }
       char* val_as_str = as_val_tostring(p_val);

        LOG("query callback returned %s", val_as_str);
        free(val_as_str);

        return true;

}
