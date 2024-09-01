package com.hmdp.service.impl;
import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        // 1、校验手机号码
        if (RegexUtils.isPhoneInvalid(phone)){
            // 2、如果不符合，返回错误信息
            return Result.fail("手机号码格式错误！");
        }
        // 3、符合生成验证码
        String code = RandomUtil.randomNumbers(6);
        // 4、保存验证码到redis
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone, code, Duration.ofMinutes(LOGIN_CODE_TTL));
        // 5、发送验证码
        log.debug(String.format("发送短信验证码成功，验证码: %s", code));

        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        // 1、校验手机号码
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)){
            // 2、如果不符合，返回错误信息
            return Result.fail("手机号码格式错误！");
        }
        // 3、校验验证码,从redis中
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        // 不一致报错
        if (cacheCode == null || !cacheCode.equals(code)){
            return Result.fail("验证码错误");
        }
        // 4、一致, 根据手机号查询用户 select * from tb_user where phone=?
        User user = query().eq("phone", phone).one();
        // 5、判断用户是否存在
        if (user == null){
            // 6、不存在，创建新用户并保存
            user = createUserWithPhone(phone);
        }

        // 7、保存用户信息到redis
        // 7.1、随机生成token，作为登录令牌
        String token = UUID.randomUUID().toString(true);
        // 7.2、转为hashmap存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create().setIgnoreNullValue(true).
                        setFieldValueEditor((fieldName, fieldValue)-> fieldValue.toString()));
        // 7.3、存储
        String key = (LOGIN_USER_KEY + token);
        System.out.println(key);
        stringRedisTemplate.opsForHash().putAll(key, userMap);

        // 7.4 设置有效期
        stringRedisTemplate.expire(key, Duration.ofMinutes(LOGIN_USER_TTL));

        return Result.ok(token);
    }

    @Override
    public Result sign() {
        // 1、获取当前登录的用户
        Long userId = UserHolder.getUser().getId();
        // 2、获取日期
        LocalDateTime now = LocalDateTime.now();
        // 3、拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern("yyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        // 4、获取当前日期，是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        stringRedisTemplate.opsForValue().setBit(key, dayOfMonth - 1, true);
        // 5、写入redis， SETBIT key offset 1
        return Result.ok();
    }

    @Override
    public Result signCount() {
        // 1、获取当前登录的用户
        Long userId = UserHolder.getUser().getId();
        // 2、获取日期
        LocalDateTime now = LocalDateTime.now();
        // 3、拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern("yyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        // 4、获取当前日期，是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        // 5、获取本月截至今天所有的签到记录，返回十进制数字
        List<Long> result = stringRedisTemplate.opsForValue().bitField(
                key, BitFieldSubCommands
                        .create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth))
                        .valueAt(0)
        );
        if (result == null || result.isEmpty()){
            return Result.ok(0);
        }
        Long num = result.get(0);
        if (num == null || num == 0){
            return Result.ok(0);
        }
        // 6、循环便利
        int count = 0;
        while(true){
            // 6.1、让这个数字和1做与运算，得到最后一个bit位
            if ((num & 1) == 0){
                // 6.3如果为0，说明未签到结束
                break;
            }else{
                // 6.4不为0，说明签到，计数器+1
                count++;
            }

            // 6.5把数字右移一位，抛弃最后一位
            num >>>= 1;
        }
        return Result.ok(count);
    }

    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        // 2、保存用户
        save(user);
        return user;
    }
}
