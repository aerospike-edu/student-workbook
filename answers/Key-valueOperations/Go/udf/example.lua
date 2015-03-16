function example_function(rec, userid, profile)
    local ret = map()                     -- Initialize the return value (a map)
    if not aerospike:exists(rec) then     -- Check to see that the record exists
      ret['status'] = 'DOES NOT EXIST'    -- Set the return status
    else
      local x = rec['bin1']               -- Get the value from record bin named "bin1"
      rec['bin2'] = (x / 2)               -- Set the value in record bin named "bin2"
      if  x < 0  then
        aerospike:remove(rec)              -- Delete the entire record
        ret['status'] = 'DELETE'           -- Populate the return status
      elseif  x > 100  then
        rec['bin3'] = nil                  -- Delete record bin named "bin3"
        ret['status'] = 'VALUE TOO HIGH'   -- Populate the return status
      else
        local myuserid = userid            -- Get the UDF argument "userid"
        local myprofile = profile          -- Get the UDF argument "profile"
        ret['status'] = 'OK'               -- Populate the return status
        ret['userdata'] = map{             -- Populate return value map
          myuserid    = userid,
          otheruserid = rec['userid'],
          match_score = getMatchScore(myprofile, rec['profile'])
          }
      end
      aerospike:update(rec)                -- Update the main record
  end
  return ret                             -- Return the Return value and/or status
end

