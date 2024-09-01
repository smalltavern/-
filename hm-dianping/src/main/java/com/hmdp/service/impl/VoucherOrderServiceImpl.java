package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService iSeckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static{
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    private IVoucherOrderService proxy;

    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    @Override
    public Result seckillVouchor(Long voucherId) {
        return null;
    }

    @Override
    public void createVoucherOrder(VoucherOrder VoucherOrder) {

    }


    private class VoucherOrderHandler implements Runnable {
        private String queueName = "stream.orders";

        @Override
        public void run() {
            while (true) {

                try {
                    // 1、获取消息队列中订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    // 2、判断消息获取是否成功
                    if (list == null || list.isEmpty()) {
                        // 2.1、如果失败，说明没有消息，继续下一次循环
                        continue;
                    }
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder order = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    // 2。2、如果成功，可以下单
                    handleVoucherOrder(order);
                    // ACK确认 SACK
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());

                } catch (Exception e) {
                    log.error("处理异常信息!", e);
                    try {
                        handlePendingList();
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
                // 2、创建订单
            }
        }

        public void handlePendingList() throws InterruptedException {
            while (true) {

                try {
                    // 1、获取pending-list中订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    // 2、判断消息获取是否成功
                    if (list == null || list.isEmpty()) {
                        // 2.1、如果失败，说明pending-list没有消息，继续下一次循环
                        break;
                    }
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder order = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    // 2。2、如果成功，可以下单
                    handleVoucherOrder(order);
                    // ACK确认 SACK
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());

                } catch (Exception e) {
                    log.error("处理异常信息!", e);
                    Thread.sleep(20);
                }

            }
        }



        private void handleVoucherOrder(VoucherOrder order) {
            Long userId = order.getUserId();
            RLock lock = redissonClient.getLock("lock:order:" + userId);

            boolean isLock = lock.tryLock();

            if (!isLock) {
                log.error("不允许重复下单");
                return;
            }
            try {
                proxy.createVoucherOrder(order);
            } finally {
                lock.unlock();
            }


        }

        @Resource
        private RedissonClient redissonClient;
//        @Override
        public Result seckillVouchor(Long voucherId) {
            // 获取用户
            Long userId = UserHolder.getUser().getId();
            // 获取订单id
            long orderId = redisIdWorker.nextId("order");
            // 1、执行lua脚本
            Long result = stringRedisTemplate.execute(
                    SECKILL_SCRIPT,
                    Collections.emptyList(),
                    voucherId.toString(),
                    userId.toString(),
                    String.valueOf(orderId)

            );

            // 2、判断结果是否为0
            int r = result.intValue();
            if (result != 0) {
                // 2.1 不为0 代表没有购买资格
                return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
            }
            // 3 返回订单id
            return Result.ok();

        }

        @Transactional
//        @Override
        public void createVoucherOrder(VoucherOrder order) {
            // 6、一人一单
            Long userId = order.getUserId();
            // 6.1 查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", order).count();
            // 6.2 是否存在
            if (count > 0) {
                log.error("用户已经购买过依次了");
                return;
            }

            boolean success = iSeckillVoucherService.update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", order)
                    .gt("stock", 0)
                    .update();
            if (!success) {
                log.error("订单不足！");
                return;
            }

            save(order);

        }
    }


    //    @Override
//    public Result seckillVouchor(Long voucherId) {
//        // 1、查询优惠卷
//        SeckillVoucher voucher = iSeckillVoucherService.getById(voucherId);
//        // 2、 判断秒杀是否开始
//        if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
//            return Result.fail("秒杀并没有开始");
//        }
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())){
//            return Result.fail("秒杀已经结束");
//        }
//        // 3、判断秒杀是否结束
//        if (voucher.getStock() < 1){
//            // 4、判断库存是否充足
//            return Result.fail("库存不足");
//        }
//
//        // 5、扣减库存
//        boolean success = iSeckillVoucherService.update()
//                .setSql("stock = stock - 1")
//                .eq("voucher_id", voucherId)
//                .gt("stock", 0)
//                .update();
//        if (!success){
//            Result.fail("订单不足");
//        }
//
//        Long userId = UserHolder.getUser().getId();
//
//        // 创建锁对象
////        SimpleRedisLock lock = new SimpleRedisLock(stringRedisTemplate, "order:" + userId);
//        //获取锁
////        boolean islock = lock.tryLock(1200);
//        //使用redisson锁结构
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        boolean islock = lock.tryLock();
//        //获取代理对象
//        if (!islock){
//            // 获取锁失败
//            return Result.fail("一个人只允许下一单");
//        }
//        try {
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        }finally {
//            lock.unlock();
//        }

//    @Override
//    public Result seckillVouchor(Long voucherId) {
//        Long userId = UserHolder.getUser().getId();
//
//        // 1、执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(),
//                userId.toString()
//        );
//        // 2、判断结果是否为0
//        int r = result.intValue();
//        if (result != 0){
//            // 2.1 不为0 代表没有购买资格
//            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
//        }
//        //  2.2 为0，代表有购买资格,
//        long orderId = redisIdWorker.nextId("order");
//        // TODO 2.3、放在阻塞队列中
//        VoucherOrder voucherOrder = new VoucherOrder();
//        // 订单id
//        voucherOrder.setId(orderId);
//        // 用户id
//        voucherOrder.setUserId(userId);
//        // 代金卷id
//        voucherOrder.setVoucherId(voucherId);
//        // 2、3创建阻塞队列中，并将他们放入
//        orderTasks.add(voucherOrder);
//
//        // 获取代理对象
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//
//        // 3 返回订单id
//        return Result.ok();
//
//    }

    //    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
//    private class VoucherOrderHandler implements Runnable{
//
//        @Override
//        public void run() {
//            while (true){
//                // 1、获取订单信息
//                try {
//                    VoucherOrder order = orderTasks.take();
//                    handleVoucherOrder(order);
//                } catch (InterruptedException e) {
//                    log.error("处理异常信息!", e);
//                }
//                // 2、创建订单
//            }
//        }
//    }

}
