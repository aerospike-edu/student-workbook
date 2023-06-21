function create_or_update (rec,arg1)
  rec['mybin']=arg1
  if( not aerospike:exists( rec ) ) then
   return aerospike:create(rec);
  else
   record.set_ttl(rec, -2)
   return aerospike:update(rec);
  end
end
