package com.lmx.match.engine;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 委托订单
 * </br>
 * 模拟产生随机价格，交易量
 *
 * @author: lucas
 * @create: 2019-10-24 13:23
 **/
@Data
@NoArgsConstructor
public class Order {
    private static AtomicLong atomicLong = new AtomicLong(1);
    private static Random random = new Random();

    private Long oid = Long.parseLong(String.format("1%010d", atomicLong.getAndIncrement()));
    private String pCode = "600000";
    private Integer bs = random.nextInt(2);
    private BigDecimal price = new BigDecimal(String.format("%.2f", 10 + Math.random()));
    private Integer volume = 100 + 100 * random.nextInt(10);
    private Long time = Clock.systemDefaultZone().millis();

    public Order(BigDecimal price, Integer volume) {
        this.price = price;
        this.volume = volume;
    }

    public String toString() {
        return "{oid=" + oid + "+price=" + price + ",volume=" + volume + "}";
    }
}
