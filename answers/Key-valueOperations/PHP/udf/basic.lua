function get_all_factors(number)                -- function definition
  local factors = {}                            -- local variable definition
  for possible_factor=1, math.sqrt(number), 1 do -- for loop
    local remainder = number%possible_factor    -- assignment (% 
    if remainder == 0 then                      -- if equal test
      local factor, factor_pair 
          = possible_factor, number/possible_factor -- multiple assignment
      table.insert(factors, factor)             -- table insert
      if factor ~= factor_pair then             -- if not equal
        table.insert(factors, factor_pair)
      end
    end
  end
  table.sort(factors)
  return factors                                -- return value
end
-- The Meaning of the Universe is 42. 
-- Let's find all of the factors driving the Universe.
the_universe = 42                               -- global variable and assignment
factors_of_the_universe = get_all_factors(the_universe) -- function call
-- Print out each factor
print("Count",  "The Factors of Life, the Universe, and Everything")
table.foreach(factors_of_the_universe, print)