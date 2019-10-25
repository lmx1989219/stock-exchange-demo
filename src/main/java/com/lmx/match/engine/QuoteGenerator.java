package com.lmx.match.engine;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 撮合、行情报价引擎
 *
 * @author: lucas
 * @create: 2019-10-24 13:33
 **/
public class QuoteGenerator {
    static List<Order> buyList = Lists.newArrayList();
    static List<Order> sellList = Lists.newArrayList();
    static Level10 level10 = new Level10();

    static boolean checkLimited(Order order) {
        //委托价格大于涨停价或者小于跌停价不能交易
        if ((Level10.upLimited != null && Level10.downLimited != null) && (order.getPrice().compareTo(Level10.upLimited) > 0
                || order.getPrice().compareTo(Level10.downLimited) < 0)) {
            System.err.printf("当前价格[%s]已经触及涨停或跌停\n", order.getPrice());
            return true;
        }
        return false;
    }

    public synchronized static void createOrder(Order wtOrder) {
        if (wtOrder != null) {
            if (checkLimited(wtOrder)) {
                return;
            }
            if (wtOrder.getBs() == 0) {
                buyList.add(wtOrder);
            } else {
                sellList.add(wtOrder);
            }
        } else {
            for (int i = 0; i < 50; i++) {
                Order order = new Order();
                if (checkLimited(order)) {
                    return;
                }
                if (order.getBs() == 0) {
                    buyList.add(order);
                } else {
                    sellList.add(order);
                }
            }
        }
        //降序：价格高的在前，时间早的在前
        buyList.sort((a, b) -> {
            if (a.getPrice().compareTo(b.getPrice()) == 0) {
                return a.getTime().compareTo(b.getTime());
            } else {
                return -a.getPrice().compareTo(b.getPrice());
            }
        });
        //升序:价格低在前，时间早的在前
        sellList.sort((a, b) -> {
            if (a.getPrice().compareTo(b.getPrice()) == 0) {
                return a.getTime().compareTo(b.getTime());
            } else {
                return a.getPrice().compareTo(b.getPrice());
            }
        });
    }

    public synchronized static void matchOrder() {
        //匹配
        Iterator<Order> buyIt = buyList.iterator();
        while (buyIt.hasNext()) {
            Order b = buyIt.next();
            //计算单笔买量是否小于等于总卖量，是则匹配
            Integer totalSell = sellList.stream().map(Order::getVolume).reduce(0, Integer::sum);
            if (b.getVolume() <= totalSell) {
//                System.out.printf("买队列=%s\n卖队列=%s\n", buyList, sellList);
                Iterator<Order> sellIt = sellList.iterator();
                while (sellIt.hasNext()) {
                    Order s = sellIt.next();
                    BigDecimal dealPrice;
                    //行情不为0，说明此时为连续竞价阶段了，以此价格为成交价
                    if (level10.getLast().floatValue() != 0) {
                        dealPrice = level10.getLast();
                        //卖价低于最新价，则以卖价成交
                        if (s.getPrice().compareTo(dealPrice) < 0) {
                            dealPrice = s.getPrice();
                        }
                    } else {//行情为0，说明是首次集中竞价，取卖价最低的为成交价并定为开盘价，同时设置涨跌停
                        Level10.open = dealPrice = s.getPrice();
                        Level10.upLimited = dealPrice.multiply(new BigDecimal(1 + 0.1)).setScale(2, RoundingMode.HALF_UP);
                        Level10.downLimited = dealPrice.multiply(new BigDecimal(1 - 0.1)).setScale(2, RoundingMode.HALF_UP);
                    }
                    //委买大于委卖价格则撮合
                    if (b.getPrice().compareTo(dealPrice) >= 0) {
                        pubQuote(dealPrice);
                        //单笔买单量小于等于卖单，则直接成交
                        if (b.getVolume().compareTo(s.getVolume()) <= 0) {
                            System.out.printf("当前oid=%s,委托价格=%s,成交价格=%s，全部成交，成交量=%s\n", b.getOid(), b.getPrice(), dealPrice, b.getVolume());
                            buyIt.remove();
                            s.setVolume(s.getVolume() - b.getVolume());
                            //卖单量为0则移除队列
                            if (s.getVolume() == 0) {
                                sellIt.remove();
                            }
                            //进行下一个买单撮合
                            break;
                        } else {//单笔买单量大于卖单，依次部分成交
                            System.out.printf("当前oid=%s,委托价格=%s,成交价格=%s，部分成交，成交量=%s\n", b.getOid(), b.getPrice(), dealPrice, s.getVolume());
                            sellIt.remove();
                            b.setVolume(b.getVolume() - s.getVolume());
                            //买单量为0（换句话说则匹配买单完成）则移除队列，进行下一个买单撮合
                            if (b.getVolume() == 0) {
                                buyIt.remove();
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * 发布10档行情
     *
     * @param price
     */
    public static void pubQuote(BigDecimal price) {
        List<Order> sellTmp = Lists.newArrayList(sellList);
        int levelB = 0, levelS = 0;
        List<Order> ordersSellGroup = Lists.newArrayList();
        //合并同价格订单
        sellTmp.stream().collect(Collectors.groupingBy(o -> o.getPrice(), Collectors.toList())).forEach((id, transfer) -> {
            transfer.stream().reduce((a, b) -> new Order(a.getPrice(), a.getVolume() + b.getVolume())).ifPresent(ordersSellGroup::add);
        });
        Collections.sort(ordersSellGroup, Comparator.comparing(Order::getPrice));
        List<Order> ordersSell = ordersSellGroup.stream().filter(order -> order.getPrice().compareTo(price) >= 0).collect(Collectors.toList());
        List<Order> ordersSell_ = ordersSell.subList(0, Math.min(ordersSell.size(), Level10.LEVEL));
        Collections.reverse(ordersSell_);
        level10.setSell5(new Quote[ordersSell_.size()]);
        for (Order s : ordersSell_) {
            //卖盘5档，升序
            level10.getSell5()[levelS++] = Quote.builder()
                    .pCode(s.getPCode())
                    .price(s.getPrice())
                    .volume(s.getVolume())
                    .bs(s.getBs())
                    .time(new Date())
                    .build();
        }

        List<Order> buyTmp = Lists.newArrayList(buyList);
        //买盘小于等于最新价
        List<Order> ordersBuyGroup = Lists.newArrayList();
        //合并同价格订单
        buyTmp.stream().collect(Collectors.groupingBy(o -> o.getPrice(), Collectors.toList())).forEach((id, transfer) -> {
            transfer.stream().reduce((a, b) -> new Order(a.getPrice(), a.getVolume() + b.getVolume())).ifPresent(ordersBuyGroup::add);
        });
        Collections.sort(ordersBuyGroup, Comparator.comparing(Order::getPrice));
        List<Order> ordersBuy = ordersBuyGroup.stream().filter(order -> order.getPrice().compareTo(price) < 0).collect(Collectors.toList());
        Collections.reverse(ordersBuy);
        List<Order> ordersBuy_ = ordersBuy.subList(0, Math.min(ordersBuy.size(), Level10.LEVEL));
        level10.setBuy5(new Quote[ordersBuy_.size()]);
        for (Order b : ordersBuy_) {
            //买盘5档，升序
            level10.getBuy5()[levelB++] = Quote.builder()
                    .pCode(b.getPCode())
                    .price(b.getPrice())
                    .volume(b.getVolume())
                    .bs(b.getBs())
                    .time(new Date())
                    .build();
        }
        System.out.printf("十档行情:\n%s", level10);
    }

    public static void main(String[] args) {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(5000L);
                    //模拟撮合
                    matchOrder();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(() -> {
            while (true) {
                try {
                    //模拟报单（准备集合竞价的订单）
                    createOrder(null);
                    Thread.sleep(60 * 1000L);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        while (true) {
            try {
                System.out.printf("请输入控制台指令,queue=查看买卖队列,quote=查看行情\n", buyList, sellList);
                BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));
                String request = bf.readLine();
                if ("queue".equals(request.toLowerCase())) {
                    System.out.printf("买队列=%s\n卖队列=%s\n", buyList, sellList);
                } else if ("quote".equals(request.toLowerCase())) {
                    System.out.printf("行情=%s\n", level10);
                } else {
                    //格式->价格，量，买卖标记 [11,10000,0] 可很容易模拟放量拉涨或者拉跌，只要价格合理，量足够大就行
                    List<String> params = Lists.newArrayList(Splitter.on(",").split(request));
                    Order order = new Order(new BigDecimal(params.get(0)), Integer.valueOf(params.get(1)), Integer.valueOf(params.get(2)));
                    synchronized (QuoteGenerator.class) {
                        //模拟参与连续竞价
                        createOrder(order);
                        matchOrder();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
