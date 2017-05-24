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
        # todo: get the user's tweetcount bin value
        # bins = 
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
        # todo: create the user record
        self.yep
      rescue Exception => e
        self.nope
        puts "Failed to create the tweet".colorize(:color => :red, :mode => :bold)
        pp e
        return
      end
      # todo: make sure the Training.get_user_key method works
      key = self.get_user_key(username)
      print "Updating the user record ≻".colorize(:color => :black, :mode => :bold)
      begin
        # todo: update the user's tweetcount and lasttweeted
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
      # todo: make sure the Training.get_user_key method works
        key = self.get_user_key(username)
        # todo: get the user's tweetcount bin value
        #rec = 
        unless rec
          puts "There is no such user #{username}".colorize(:red)
          return
        end
        bins = rec.bins
        tweet_count = bins.nil? ? 0 : bins['tweetcount']
        keys = []
        (1..tweet_count).each do |i|
          # todo: make sure the Training.get_tweet_key method works
          keys.push(self.get_tweet_key(username, i))
        end
        print "Batch-reading the user's tweets ≻".colorize(:color => :black, :mode => :bold)
        begin
          # todo: perform a batch read of the user's tweets
          # recs = 
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
        # todo: scan the set test.tweets and print each tweet in it
      rescue
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
