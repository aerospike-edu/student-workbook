local function collect_tweets(tweetsMap,rec)
  list.append(tweetsMap.tweets,rec.tweet)
  return tweetsMap
end

local function merge_tweets(a,b)
  a.tweets = a.tweets + b.tweets
  return a
end

function recent_tweets(stream)
   return stream : aggregate(map{tweets=list()},collect_tweets) : reduce(merge_tweets)
end
