local function my_aggregation_fn(result, rec)
  -- Initialize the age_sum with 0 and accumulate each incoming rec's age value
  -- If the count of rec itmes does not exist, initialize it with 1
  -- Else increment the existing counter
  result['age_sum'] = (result['age_sum'] or 0) + rec['age']
  result['count'] = (result['count'] or 0) + 1
  return result
end

local function my_reduce_fn(global_agg, next_agg)
  global_agg['count'] = global_agg['count'] + next_agg['count']
  global_agg['age_sum'] = global_agg['age_sum'] + next_agg['age_sum']
  return global_agg
end

function getAgeAggregates(stream, min_age, gen)
  local function my_filter_fn(rec)
    if rec['age'] > min_age and rec['gender'] == gen then
      return true
    else
      return false
    end
  end
  return stream:filter(my_filter_fn):aggregate(map{age_sum=0, count=0}, my_aggregation_fn):reduce(my_reduce_fn)
end