-- This updates a hash, based on a partial set of keys 
-- ARGV = [key, hkey1, hval1, (hkey2, hval2, ...)]
--
-- This will make the minimal changes required, not triggering
-- hset notifications for key<hkey:hval> pairs that arent modified

local hmset = {}

for i=2, #ARGV, 2 do
 local value = redis.call('HGET', ARGV[1], ARGV[i])
 if value ~= ARGV[i+1] then
  hmset[#hmset + 1] = ARGV[i]
  hmset[#hmset + 1] = ARGV[i+1]
 end
end

if #hmset > 0 then
    redis.call('HMSET', ARGV[1], unpack(hmset))
end
return 'OK'
