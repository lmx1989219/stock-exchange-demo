package com.lmx.match.engine;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * 撮合、行情报价引擎
 *
 * @author: lucas
 * @create: 2019-10-24 13:33
 **/
public class QuoteGenerator {


    static String market = "sh", pCode = "600000";
    static public SystemBus systemBus = new SystemBus();

    public static void main(String[] args) {
        //初始化市场合约代码
        Map<String, List<String>> marketPCodeList = Maps.newConcurrentMap();
        marketPCodeList.put(market, Lists.newArrayList(pCode));
        //初始化撮合池
        MatchPool matchPool = new MatchPool();
        marketPCodeList.forEach((market, pCodeList) -> pCodeList.forEach(pCode -> {
            matchPool.initPool(pCode, new OrderQueue());
        }));
        //注册撮合事件处理器
        MatchEventDispatcher matchEventDispatcher = new MatchEventDispatcher(matchPool, Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2));
        systemBus.registryEvent(matchEventDispatcher);
        //开启报单线程
        new Thread(() -> {
            while (true) {
                try {
                    matchPool.queueMap.forEach((pCode, queue) -> {
                        //模拟报单（准备集合竞价的订单）
                        for (int i = 0; i < 50; i++) {
                            Order order = new Order();
                            order.setPCode(pCode);
                            //发布挂单事件
                            systemBus.publishEvent(order);
                        }
                        //发布撮合事件
                        systemBus.publishEvent(new MatchEventDispatcher.MatchEvent(pCode, MatchEventDispatcher.EventEnum.MATCH_TRADE));
                    });

                    Thread.sleep(30 * 1000L);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        while (true) {
            try {
                System.out.printf("请输入控制台指令,queue=查看买卖队列,quote=查看行情\n");
                BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));
                String request = bf.readLine();
                if ("queue".equals(request.toLowerCase())) {
                    OrderQueue queue = matchPool.getQueue(pCode);
                    List<Order> buyList = queue.buyList;
                    List<Order> sellList = queue.sellList;
                    System.out.printf("买队列=%s\n卖队列=%s\n", buyList, sellList);
                } else if ("quote".equals(request.toLowerCase())) {
                    System.out.printf("行情=%s\n", Level10.Pool.getLevel10(pCode));
                } else {
                    //格式->价格，量，买卖标记 [11,10000,0] 可很容易模拟放量拉涨或者拉跌，只要价格合理，量足够大就行
                    List<String> params = Lists.newArrayList(Splitter.on(",").split(request));
                    Order order = new Order(new BigDecimal(params.get(0)), Integer.valueOf(params.get(1)), Integer.valueOf(params.get(2)));
                    //模拟参与连续竞价
                    //发布挂单事件,模拟报单（准备集合竞价的订单）
                    systemBus.publishEvent(order);
                    //触发撮合
                    systemBus.publishEvent(new MatchEventDispatcher.MatchEvent(order.getPCode(), MatchEventDispatcher.EventEnum.MATCH_TRADE));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
