<?php
/*******************************************************************************
 * Copyright 2012-2015 by Aerospike.
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
 *
 * @category   Training
 * @author     Ronen Botzer <rbotzer@aerospike.com>
 * @copyright  Copyright 2012-2015 Aerospike, Inc.
 * @filesource
 */
namespace Aerospike\Training;
use Aerospike;

class TweetService extends BaseService {
    /**
     * Constructor
     *
     * @param Aerospike $client
     * @param array $config
     */
    public function __construct(Aerospike $client, array $config, $annotate = false) {
        parent::__construct($client, $config, $annotate);
    }

    /* Data Model
     * Namespace: test
     * Set: tweets
     * Key: <username:<counter>>
     * Bins:
     * tweet - string 
     * ts - int (Stores epoch timestamp of the tweet)
     * username - string
     * Sample Key: dash:1
     * Sample Record:
     * { tweet: 'Put. A. Bird. On. It.',
     *   ts: 1408574221,
     *   username: 'dash'
     * }
     */

    /**
     * Asks the developer for input, then creates a new user in the database
     * as a record in namespace 'test', set 'users'
     * @param string|null $username optionally provide the username
     * @return boolean whether the tweet was created
     * @throws \Aerospike\Training\Exception
     */
    public function createTweet($username = null) {
        echo colorize("\nCreate a tweet\n", 'blue', true);
        if ($username == '') {
            // Get username
            echo colorize("Enter username (or hit Return to skip): ");
            $username = trim(readline());
        }
        if ($username != '') {
            $record = $this->getUser($username);
            $ubins = $record['bins'];
            $tweet_count = isset($ubins['tweetcount']) ? $ubins['tweetcount'] : 0;
            $tweet_count++;

            $bins = array();
            // Get a tweet
            echo colorize("Enter tweet for $username: ");
            $bins['tweet'] = trim(readline());
            $bins['ts'] = time() * 1000;
            $bins['username'] = $username;
            echo colorize("Creating tweet record ≻", 'black', true);
            $key = $this->getTweetKey($username, $tweet_count);
            $status = $this->client->put($key, $bins);
            if ($status !== Aerospike::OK) {
                // throwing an \Aerospike\Training\Exception
                echo fail();
                throw new Exception($this->client, "Failed to create the tweet");
            }
            echo success();
            return $this->updateUser($username, $bins['ts'], $tweet_count);
        }
        return false;
    }

    /**
     * Asks the developer for a username, then fetches the user's tweets
     * @param string|null $username optionally provide the username
     * @return boolean whether the operation succeeded
     * @throws \Aerospike\Training\Exception
     */
    public function batchGetTweets($username = null) {
        echo colorize("\nGet the user's tweets\n", 'blue', true);
        if ($username == '') {
            // Get username
            echo colorize("Enter username (or hit Return to skip): ");
            $username = trim(readline());
        }
        if ($username != '') {
            $record = $this->getUser($username, array('tweetcount'));
            $tweet_count = intval($record['bins']['tweetcount']);
            if ($this->annotate) display_code(__FILE__, __LINE__, 4);
            $keys = array();
            for ($i = 1; $i <= $tweet_count; $i++) {
                $keys[] = $this->getTweetKey($username, $i);
            }
            echo colorize("Batch-reading the user's tweets ≻", 'black', true);
            if ($this->annotate) display_code(__FILE__, __LINE__, 6);
            $status = $this->client->getMany($keys, $records);
            if ($status !== Aerospike::OK) {
                echo fail();
                // throwing an \Aerospike\Training\Exception
                throw new Exception($this->client, "Failed to batch-read the tweets for $username");
            }
            echo success();
            echo colorize("Here are $username's tweets:\n", 'blue', true);
            foreach ($records as $record) {
                echo colorize($record['bins']['tweet'], 'black')."\n";
            }
            return true;
        } else {
            throw new \Exception("Invalid input provided for username in UserService::getUser()");
        }
    }

    /**
     * Displays all tweets using a scan operation
     * @throws \Aerospike\Training\Exception
     */
    public function scanAllTweets() {
        echo colorize("\nScan for tweets\n", 'blue', true);
        echo colorize("Pass scanned records to callback ≻\n", 'black', true);
        if ($this->annotate) display_code(__FILE__, __LINE__, 7);
        $status = $this->client->scan('test','tweets', function ($record) {
            var_dump($record['bins']['tweet']);
        }, array('tweet'));
        if ($status !== Aerospike::OK) {
            // throwing an \Aerospike\Training\Exception
            throw new Exception($this->client, "Failed to scan test.tweets");
        }
        echo colorize("Done", 'green', true). success()."\n";
    }

    /**
     * Asks the developer for a username, then queries for the user's tweets
     * @throws \Aerospike\Training\Exception
     */
    public function queryTweetsByUsername() {
        echo colorize("\nQuery for the user's tweets", 'blue', true)."\n";
        echo colorize("Ensuring there is an index on the username bin of test.tweets ≻", 'black', true);
        $ok = $this->ensureIndex('test','tweets','username','tweets_username_idx',Aerospike::INDEX_TYPE_DEFAULT, Aerospike::INDEX_STRING);
        if ($ok) echo success();
        else echo fail();

        // Get username
        echo colorize("Enter username (or hit Return to skip): ");
        $username = trim(readline());
        if ($username == '') return;
        echo colorize("Pass queried records to callback ≻", 'black', true)."\n";
        if ($this->annotate) display_code(__FILE__, __LINE__, 8);
        $where = $this->client->predicateEquals('username', $username);
        $status = $this->client->query('test','tweets', $where, function ($record) {
            var_dump($record['bins']['tweet']);
        }, array('tweet'));
        if ($status !== Aerospike::OK) {
            // throwing an \Aerospike\Training\Exception
            throw new Exception($this->client, "Failed to query test.tweets");
        }
        echo colorize("Done", 'green', true). success()."\n";
    }

    /**
     * Asks the developer for a range, then queries for users by their tweet count
     * @throws \Aerospike\Training\Exception
     */
    public function queryUsersByTweetCount() {
        echo colorize("\nQuery for users by their tweet count", 'blue', true)."\n";
        echo colorize("Ensuring there is an index on the tweetcount bin of test.users ≻", 'black', true);
        $ok = $this->ensureIndex('test','users','tweetcount','users_tweetcount_idx',Aerospike::INDEX_TYPE_DEFAULT, Aerospike::INDEX_NUMERIC);
        if ($ok) echo success();
        else echo fail();

        echo colorize("Enter min tweet count (or hit Return to skip): ");
        $min = trim(readline());
        if ($min == '') return;
        echo colorize("Enter max tweet count: ");
        $max = trim(readline());
        echo colorize("Pass queried records to callback ≻", 'black', true)."\n";
        if ($this->annotate) display_code(__FILE__, __LINE__, 8);
        $where = $this->client->predicateBetween('tweetcount', $min, $max);
        $status = $this->client->query('test','users', $where, function ($rec) {
            echo colorize("{$rec['bins']['username']} has {$rec['bins']['tweetcount']} tweets", 'black')."\n";
        });
        if ($status !== Aerospike::OK) {
            // throwing an \Aerospike\Training\Exception
            throw new Exception($this->client, "Failed to query test.users");
        }
        echo colorize("Done", 'green', true). success()."\n";
    }

    /**
     * Updates the user with the tweet count and latest tweet info
     * @param string $username
     * @param int $ts
     * @param int $tweet_count
     * @return boolean whether the update succeeded
     * @throws \Aerospike\Training\Exception
     */
    private function updateUser($username, $ts, $tweet_count) {
        $key = $this->getUserKey($username);
        $bins = array('tweetcount' => $tweet_count, 'lasttweeted' => $ts);
        echo colorize("Updating the user record ≻", 'black', true);
        if ($this->annotate) display_code(__FILE__, __LINE__, 6);
        $status = $this->client->put($key, $bins);
        if ($status !== Aerospike::OK) {
            echo fail();
            // throwing an \Aerospike\Training\Exception
            throw new Exception($this->client, "Failed to get the user $username");
        }
        echo success();
        return true;
    }

}

?>
