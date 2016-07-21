$:.push File.expand_path('.')
require 'shared'
require 'pp'

include Aerospike
include Training

def main
  puts "***** Welcome to Aerospike Developer Training *****".colorize(:color => :blue, :mode => :bold);
  print "Connecting to Aerospike cluster ≻".colorize(:color => :black, :mode => :bold)
  begin
    Training.init
  rescue Exception => e
    puts " [✗] ".colorize(:color => :red, :mode => :bold)
    puts "Connection to Aerospike cluster failed! Please check the server settings and try again!".colorize(:color => :red, :mode => :bold)
    Training.print_params
    exit(1)
  end
  puts " [✓] ".colorize(:color => :green, :mode => :bold)
  selection = Training.show_menu
  case selection
    when 1
      create_user(Training.client)
      create_tweet(Training.client)
    when 2
      get_user(Training.client)
    when 3
      batch_get_tweets(Training.client)
    when 4
      scan_tweets(Training.client)
    when 5
      update_password(Training.client)
    when 6
      query_tweets(Training.client)
      query_by_tweetcount(Training.client)
    when 7
      aggregate_by_region(Training.client)
  end
  Training.client.close
end

main
