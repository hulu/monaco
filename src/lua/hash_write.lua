-- This overwrites a hash - intended for small hashes only
-- ARGV = [key, hkey1, hval1, (hkey2, hval2, ...)]
--
-- This will make the minimal changes required, not triggering
-- hset notifications for key<hkey:hval> pairs that arent modified
-- or hdel notifications unecessarily

local setkeys = {}
local hmset = {}

for i=2, #ARGV, 2 do
 local value = redis.call('HGET', ARGV[1], ARGV[i])
 if value ~= ARGV[i+1] then
  hmset[#hmset + 1] = ARGV[i]
  hmset[#hmset + 1] = ARGV[i+1]
 end
 setkeys[ARGV[i]] = true  
end

local hkeys = redis.call('HKEYS', ARGV[1])
local hdel = {}
for i=1, #hkeys do
 if not setkeys[hkeys[i]] then 
  hdel[#hdel + 1] = hkeys[i]
 end
end

if #hdel > 0 then
    redis.call('HDEL', ARGV[1], unpack(hdel))
end
if #hmset > 0 then
    redis.call('HMSET', ARGV[1], unpack(hmset))
end
return 'OK'
