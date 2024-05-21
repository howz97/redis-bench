local n = redis.call('HGET', KEYS[1], KEYS[2])
if not n then
    error("ENOENT")
end
return redis.call('GET', 'bar' .. string.format("%d", n))