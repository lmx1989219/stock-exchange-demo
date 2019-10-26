package com.lmx.match.engine;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * 撮合池
 *
 * @author : lucas
 * @date : 2019/10/26 15:51
 */
public class MatchPool {
    Map<String, OrderQueue> queueMap = Maps.newConcurrentMap();
    ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

    enum DealType {
        ALL,    //全部成交
        PART    //部分成交
    }

    static Level10.Pool level10Pool = new Level10.Pool();

    public void initPool(String pCode, OrderQueue queue) {
        queueMap.put(pCode, queue);
    }

    public OrderQueue getQueue(String pCode) {
        return queueMap.get(pCode);
    }

    Level10 initQuote(String pCode, BigDecimal dealPrice) {
        //行情为空说明是首次集中竞价，取卖价最低的为成交价并定为开盘价，同时设置涨跌停
        if (level10Pool.getLevel10(pCode) == null) {
            Level10 level10 = new Level10();
            level10.setPCode(pCode);
            level10.setOpen(dealPrice);
            level10.setUpLimited(dealPrice.multiply(new BigDecimal(1 + 0.1)).setScale(2, RoundingMode.HALF_UP));
            level10.setDownLimited(dealPrice.multiply(new BigDecimal(1 - 0.1)).setScale(2, RoundingMode.HALF_UP));
            level10Pool.addPool(pCode, level10);
            return level10;
        } else {
            return level10Pool.getLevel10(pCode);
        }
    }

    public void matchOrder(String pCode) {
        executorService.execute(() -> {
            OrderQueue queue = queueMap.get(pCode);
            synchronized (queue.lock) {
                List<Order> buyList = queue.buyList;
                List<Order> sellList = queue.sellList;
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
                            BigDecimal dealPrice = s.getPrice();
                            Level10 level10 = initQuote(b.getPCode(), dealPrice);
                            //行情不为0，说明此时为连续竞价阶段了，以此价格为成交价
                            if (level10.getLast().floatValue() != 0) {
                                dealPrice = level10.getLast();
                                //卖价低于最新价，则以卖价成交
                                if (s.getPrice().compareTo(dealPrice) < 0) {
                                    dealPrice = s.getPrice();
                                }
                            }
                            //委买大于委卖价格则撮合
                            if (b.getPrice().compareTo(dealPrice) >= 0) {
                                pubQuote(queue, b.getPCode(), dealPrice);
                                //单笔买单量小于等于卖单，则直接成交
                                if (b.getVolume().compareTo(s.getVolume()) <= 0) {
                                    dealCallback(DealType.ALL, b, s, dealPrice);
                                    buyIt.remove();
                                    s.setVolume(s.getVolume() - b.getVolume());
                                    //卖单量为0则移除队列
                                    if (s.getVolume() == 0) {
                                        sellIt.remove();
                                    }
                                    //进行下一个买单撮合
                                    break;
                                } else {//单笔买单量大于卖单，依次部分成交
                                    dealCallback(DealType.PART, b, s, dealPrice);
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
        });
    }

    /**
     * 发布10档行情
     *
     * @param price
     */
    void pubQuote(OrderQueue queue, String pCode, BigDecimal price) {
        List<Order> buyList = queue.buyList;
        List<Order> sellList = queue.sellList;
        Level10 level10 = level10Pool.getLevel10(pCode);
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


    /**
     * 推送成交信息
     *
     * @param type
     * @param b
     * @param s
     * @param dealPrice
     */
    void dealCallback(DealType type, Order b, Order s, BigDecimal dealPrice) {
        if (type == DealType.PART)
            System.out.printf("当前oid=%s,委托价格=%s,成交价格=%s，部分成交，成交量=%s\n", b.getOid(), b.getPrice(), dealPrice, s.getVolume());
        else if (type == DealType.ALL)
            System.out.printf("当前oid=%s,委托价格=%s,成交价格=%s，全部成交，成交量=%s\n", b.getOid(), b.getPrice(), dealPrice, b.getVolume());

    }
}
