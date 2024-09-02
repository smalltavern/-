-- 比较线程标识和
if (redis.call('get', KEYS[1]) == ARGV[1]) then
    return redis.call('del', KEYS[1])
end

-- 返回锁的值
return 0