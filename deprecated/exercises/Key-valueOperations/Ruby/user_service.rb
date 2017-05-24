require 'pp'

module Training
  module UserService

    # Data Model
    # Namespace: test
    # Set: users
    # Key: <username>
    # Bins:
    # username - String
    # password - String (For simplicity password is stored in plain-text)
    # gender - String (Valid values are 'm' or 'f')
    # region - String (Valid values are: 'n' (North), 's' (South), 'e' (East), 'w' (West) -- to keep data entry to minimal we just store the first letter)
    # lasttweeted - int (Stores epoch timestamp of the last / most recent tweet) -- Default to 0
    # tweetcount - int (Stores total number of tweets for the user) -- Default to 0
    # interests - Array of interests
    # Sample Key: dash
    # Sample Record:
    # { :username => 'dash',
    #   :password => 'dash',
    #   :gender => 'm',
    #   :region => 'w',
    #   :lasttweeted => 1408574221,
    #   :tweetcount => 20,
    #   :interests => ['photography', 'technology', 'dancing', 'house music]
    # }

    def create_user(client)
      puts "\nCreate a user".colorize(:color => :blue, :mode => :bold)
      print 'Enter username (or hit Return to skip): '.colorize(:blue)
      username = gets.chomp
      bins = { 'username': username }
      return if username.length < 1
      print "Enter password for #{username}: ".colorize(:blue)
      bins['password'] = gets.chomp
      print "Select gender (f or m) for #{username}: ".colorize(:blue)
      bins['gender'] = gets.chomp
      print "Select region (north, south, east or west) for #{username}: ".colorize(:blue)
      bins['region'] = gets.chomp
      print "Enter comma-separated interests for #{username}: ".colorize(:blue)
      bins['interests'] = gets.chomp.split(',')

      print "Creating user record ≻".colorize(:color => :black, :mode => :bold)
      begin
        # todo: write a new record at a key tuple ('test', 'users', username)
        #key =
        self.yep
      rescue
        self.nope
        puts "Connection to Aerospike cluster failed! Please check the server settings and try again!".colorize(:color => :red, :mode => :bold)
      end
    end

    def check_and_set_password(client)
      puts "Update a user's password using CAS".colorize(:color => :blue, :mode => :bold)
      print 'Enter username (or hit Return to skip): '.colorize(:blue)
      username = gets.chomp
      return if username == ''
      print 'Enter a new password: '.colorize(:blue)
      new_password = gets.chomp
      print "Getting the metadata for the record ≻".colorize(:color => :black, :mode => :bold)
      begin
        # todo: get the generation metadata for the user's record
      rescue
      end
      print "Updating the user's password if the generation matches ≻".colorize(:color => :black, :mode => :bold)
      begin
        # todo: update the user's password using check-and-set against the record generation
      rescue Exception => e
      end
    end

    def get_user(client)
      puts "\nCreate a user".colorize(:color => :blue, :mode => :bold)
      print 'Enter username (or hit Return to skip): '.colorize(:blue)
      username = gets.chomp
      # todo: make sure the Training.get_user_key method works
      key = self.get_user_key(username)
      # todo: read the user record
      #rec = 
      if rec
        puts "Record.key (Key)".colorize(:mode => :bold)
        pp rec.key
        puts "Record.generation (Fixnum)".colorize(:mode => :bold)
        pp rec.generation
        puts "Record.expiration (Fixnum)".colorize(:mode => :bold)
        pp rec.expiration
        puts "Record.bins (Hash)".colorize(:mode => :bold)
        pp rec.bins
      else
        puts "There is no such user #{username}".colorize(:red)
      end
    end
  end
end
