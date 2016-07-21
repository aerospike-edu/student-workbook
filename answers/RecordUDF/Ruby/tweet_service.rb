module Training
  module TweetService

    # Namespace: test
    # Set: tweets
    # Key: <username:<counter>>
    # Bins:
    # tweet - String
    # ts - int (Stores epoch timestamp of the tweet)
    # username - String
    # Sample Key: dash:1
    # Sample Record:
    # { :tweet => 'Put. A. Bird. On. It.',
    #   :ts => 1408574221,
    #   :username => 'dash'
    # }

    def create_tweet(client, username = nil)
      puts "\nCreate a tweet".colorize(:color => :blue, :mode => :bold)
      unless username
        print "Enter username (or hit Return to skip): ".colorize(:blue)
        username = gets.chomp
      end
      unless username == ''
        key = self.get_user_key(username)
        bins = client.get(key, ['tweetcount']).bins
        tweet_count = bins.nil? ? 1 : bins['tweetcount'] + 1
      end
      ts = (Time.now.to_f * 1000).round
      bins = {'ts' => ts}
      print "Enter tweet for #{username}: ".colorize(:blue)
      bins['tweet'] = gets.chomp
      bins['username'] = username
      print "Creating tweet record ≻".colorize(:color => :black, :mode => :bold)
      key = self.get_tweet_key(username, tweet_count)
      begin
        client.put(key, bins)
        self.yep
      rescue Exception => e
        self.nope
        puts "Failed to create the tweet".colorize(:color => :red, :mode => :bold)
        pp e
        return
      end
      key = self.get_user_key(username)
      bins = {'tweetcount' => tweet_count, 'lasttweeted' => ts}
      print "Updating the user record ≻".colorize(:color => :black, :mode => :bold)
      begin
        client.put(key, bins)
        self.yep
      rescue
        self.nope
        print "Failed to update the user record ≻".colorize(:color => :black, :mode => :bold)
      end
    end

    def batch_get_tweets(client, username = nil)
      puts "\nGet the user's tweet".colorize(:color => :blue, :mode => :bold)
      unless username
        print "Enter username (or hit Return to skip): ".colorize(:blue)
        username = gets.chomp
      end
      unless username == ''
        key = self.get_user_key(username)
        rec = client.get(key, ['tweetcount'])
        unless rec
          puts "There is no such user #{username}".colorize(:red)
          return
        end
        bins = rec.bins
        tweet_count = bins.nil? ? 0 : bins['tweetcount']
        keys = []
        (1..tweet_count).each do |i|
          keys.push(self.get_tweet_key(username, i))
        end
        print "Batch-reading the user's tweets ≻".colorize(:color => :black, :mode => :bold)
        begin
          recs = client.batch_get(keys)
          self.yep
        rescue Exception => e
          self.nope
          puts "Failed to batch-read the tweets for #{username}".colorize(:color => :red, :mode => :bold)
          pp e
          return
        end
        puts "Here are #{username}'s tweets:".colorize(:color => :blue, :mode => :bold)
        recs.each do |r|
          puts r.bins['tweet']
        end
      end
    end

    def scan_tweets(client)
      puts "\nScan for tweets".colorize(:color => :blue, :mode => :bold)
      begin
        policy = ScanPolicy.new(:fail_on_cluster_change => true)
        recordset = client.scan_all(self.namespace, 'tweets', [], policy)

        recordset.each do |rec|
          puts rec.bins['tweet']
        end
      rescue
        puts "Failed to scan test.tweets".colorize(:color => :red, :mode => :bold)
      end
    end

    def query_tweets(client)
      puts "Query for the user's tweets".colorize(:color => :blue, :mode => :bold)
      puts "This is part of the Query exercises".colorize(:color => :red, :mode => :bold)
    end

    def query_by_tweetcount(client)
      puts "Query for user by their tweet count".colorize(:color => :blue, :mode => :bold)
      puts "This is part of the Query exercises".colorize(:color => :red, :mode => :bold)
    end

    def aggregate_by_region(client)
      puts "Aggregate user's by region whose tweet count is in a given range".colorize(:color => :blue, :mode => :bold)
      puts "This is part of the Aggregations exercises".colorize(:color => :red, :mode => :bold)
    end

  end
end
