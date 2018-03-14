function rec_touch(rec)
 -- Update the record without changing the TTL
 record.set_ttl(rec, -2)
 -- UDF requirement: must touch at least one bin to update a record
 local bin_names = record.bin_names(rec)
 local do_nothing = rec[bin_names[1]]
 rec[bin_names[1]] = do_nothing
 aerospike:update(rec)
end


