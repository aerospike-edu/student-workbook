
local function aggregate_for_average(cumulative, next)
  cumulative['total'] = cumulative['total'] + next['fixedPoint'] 
  cumulative['count'] = cumulative['count'] + 1
  debug("Cumulative: "..tostring(type(cumulative)) .. ":" ..tostring(cumulative))
  return cumulative
end

local function map_for_average(element)
  local count = element['count']
  local total = element['total']
  local average = total / count
  debug("map_for_average -- Total: " .. tostring(total).. " Count: " ..tostring(count).." Average: "  ..tostring(average))
  return average
end

local function reduce_for_average(a, b)
  return (a + b) /2 
end

function average_double(stream)

  return stream : aggregate(map{total=0, count=0}, aggregate_for_average) : map(map_for_average) : reduce(reduce_for_average)

