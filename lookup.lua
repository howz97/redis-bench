local buf = redis.call('HGET', KEYS[1], KEYS[2])
if not buf then
    error("ENOENT")
end
local ino = struct.unpack(">I8", string.sub(buf, 2))
-- double float has 52 significant bits
if ino > 4503599627370495 then
    error("ENOTSUP")
end
return redis.call('GET', 'inode' .. string.format("%d", ino))