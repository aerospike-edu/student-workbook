function updateName(topRec,name)
   -- Log current name
   debug("current name: " .. topRec['name'])
   -- Assign new name to the user record
   topRec['name'] = name
   -- Update user record
   aerospike:update(topRec)
   -- Log new password
   debug("new name: " .. topRec['name'])
   -- return new name
   return topRec['name']
end
