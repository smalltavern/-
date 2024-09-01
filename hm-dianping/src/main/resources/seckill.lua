-- 1、参数列表，
-- 1.1优惠卷id
local voucherId = ARGV[1]
-- 1.2用户id
local userId = ARGV[2]
-- 1.3 订单id
local orderId = ARGV[2]

-- 2数据key
-- 2.1库存key
local stockKey = 'seckill:stock:'..voucherId
-- 2.2订单key
local orderKey = 'seckill:order:'..voucherId


--业务脚本
-- 判断voucher是否充足
if (tonumber(redis.call('get', stockKey)) <= 0) then
    -- 库存不足返回1
    return 1
end

-- 判断用户是否下单
if (redis.call('sismember', orderKey, userId) == 1) then
    return 2
end
-- 扣除库存
redis.call('incrby', stockKey, -1)
-- 下单
redis.call('sadd', orderKey, userId)

-- 发送消息到stream队列当中
redis.call('sadd', 'stream.orders', '*', 'userId', userId, 'voucherId', voucherId, 'id', orderId)

return 0