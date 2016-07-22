$:.push File.expand_path('.')
require 'optparse'
require 'user_service'
require 'tweet_service'

require 'rubygems'
require 'aerospike'
require 'colorize'

module Training
  include Training::UserService
  include Training::TweetService

  attr_accessor :write_policy, :policy, :client, :logger

  def init
    @@options = {
      :host => '127.0.0.1',
      :port => 3000,
      :namespace => 'test',
      :set => 'users',
    }

    opt_parser = OptionParser.new do |opts|
      opts.banner = "Usage: #{$0} [#{@@options}]"

      opts.on("-h", "--host HOST", "Aerospike server seed hostnames or IP addresses") do |v|
        @@options[:host] = v
      end

      opts.on("-p", "--port PORT", "Aerospike server seed hostname or IP address port number.") do |v|
        @@options[:port] = v.to_i
      end

      opts.on("-n", "--namespace NAMESPACE", "Aerospike namespace.") do |v|
        @@options[:namespace] = v
      end

      opts.on("-s", "--set SET", "Aerospike set name.") do |v|
        @@options[:set] = v
      end

      opts.on_tail("-u", "--help", "Show usage information.") do |v|
        puts opts
        exit
      end
    end # opt_parser
    opt_parser.parse!

    @write_policy = WritePolicy.new
    @policy = Policy.new
    @logger = Logger.new(STDOUT, Logger::INFO)
    @client = host ? Client.new(Host.new(host, port)) : Client.new
  end

  def host
    @@options[:host]
  end

  def port
    @@options[:port]
  end

  def namespace
    @@options[:namespace]
  end

  def set_name
    @@options[:set]
  end

  def get_user_key(username)
    Key.new(self.namespace, self.set_name, username)
  end

  def get_tweet_key(username, id)
    Key.new(self.namespace, 'tweets', "#{username}:#{id}")
  end

  def nope
    puts " [✗] ".colorize(:color => :red, :mode => :bold)
  end

  def yep
    puts " [✓] ".colorize(:color => :green, :mode => :bold)
  end

  def print_params
    @logger.info("hosts:\t\t#{@@options[:host]}")
    @logger.info("port:\t\t#{@@options[:port]}")
    @logger.info("namespace:\t#{@@options[:namespace]}")
    @logger.info("set:\t\t#{@@options[:set]}")
  end

  def show_menu
    puts "\nWhat would you like to do:".colorize(:color=>:blue, :mode=>:bold)
    puts "1> Create A User And A Tweet".colorize(:blue)
    puts "2> Read A User Record".colorize(:blue)
    puts "3> Batch Read Tweets For A User".colorize(:blue)
    puts "4> Scan All Tweets For All Users".colorize(:blue)
    puts "5> Record UDF -- Update User Password".colorize(:blue)
    puts "6> Query Tweets By Username And Users By Tweet Count Range".colorize(:blue)
    puts "7> Stream UDF -- Aggregation Based on Tweet Count By Region".colorize(:blue)
    puts "0> Exit".colorize(:blue)
    print "\nSelect 0-7 and hit enter: ".colorize(:blue)
    gets.chomp.to_i
  end

end
