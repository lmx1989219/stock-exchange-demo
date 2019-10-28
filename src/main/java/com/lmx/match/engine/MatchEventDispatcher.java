package com.lmx.match.engine;

import com.google.common.collect.Lists;
import com.google.common.eventbus.Subscribe;
import lombok.AllArgsConstructor;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * 事件处理器
 *
 * @author: lucas
 * @create: 2019-10-28 09:49
 **/
@AllArgsConstructor
public class MatchEventDispatcher {
    MatchPool matchPool;
    ExecutorService executorService;

    @Subscribe
    public void subOrder(Order order) {
        matchPool.getQueue(order.getPCode()).addOrder(order);
    }

    @Subscribe
    public void subMatchEvent(MatchEvent matchEvent) {
        matchPool.matchOrder(matchEvent.pCode);
    }

    @Subscribe
    public void subQuote(QuoteEvent quoteEvent) {
        //便于直观查看用单线程
        pushQuote(quoteEvent.orderQueue, quoteEvent.pCode, quoteEvent.last);
//        executorService.execute(() -> pushQuote(quoteEvent.orderQueue, quoteEvent.pCode, quoteEvent.last));
    }

    @Subscribe
    public void subDealOrder(DealEvent dealEvent) {
        //便于直观查看用单线程
        dealCallback(dealEvent.dealType, dealEvent.b, dealEvent.s, dealEvent.price);
//        executorService.execute(() -> dealCallback(dealEvent.dealType, dealEvent.b, dealEvent.s, dealEvent.price));
    }

    enum EventEnum {
        ORDER_ADD,//挂单
        ORDER_REVOKE,//撤单
        MATCH_TRADE,//撮合
        PUB_QUOTE,//发布行情
        DEAL_CALLBACK//成交回报
    }

    @AllArgsConstructor
    static class MatchEvent {
        private String pCode;
        private EventEnum eType;
    }

    @AllArgsConstructor
    static class QuoteEvent {
        private OrderQueue orderQueue;
        private String pCode;
        private BigDecimal last;
        private EventEnum eType;
    }

    @AllArgsConstructor
    static class DealEvent {
        private MatchPool.DealType dealType;
        private Order b, s;
        private BigDecimal price;
        private EventEnum eType;
    }

    /**
     * 发布10档行情
     *
     * @param price
     */
    void pushQuote(OrderQueue queue, String pCode, BigDecimal price) {
        List<Order> buyList = queue.buyList;
        List<Order> sellList = queue.sellList;
        Level10 level10 = Level10.Pool.getLevel10(pCode);
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
        level10.setSell5(new Quote[Level10.LEVEL]);
        for (int i = 0; i < Level10.LEVEL; i++) {
            if (i < ordersSell_.size()) {
                Order s = ordersSell_.get(i);
                //卖盘5档，升序
                level10.getSell5()[levelS++] = Quote.builder()
                        .pCode(s.getPCode())
                        .price(s.getPrice())
                        .volume(s.getVolume())
                        .bs(s.getBs())
                        .time(new Date())
                        .build();
            } else {
                level10.getSell5()[levelS++] = Quote.builder()
                        .pCode(pCode)
                        .price(BigDecimal.ZERO)
                        .volume(0)
                        .bs(0)
                        .time(new Date())
                        .build();
            }
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
        level10.setBuy5(new Quote[Level10.LEVEL]);
        for (int i = 0; i < Level10.LEVEL; i++) {
            if (i < ordersBuy_.size()) {
                Order b = ordersBuy_.get(i);
                //买盘5档，升序
                level10.getBuy5()[levelB++] = Quote.builder()
                        .pCode(b.getPCode())
                        .price(b.getPrice())
                        .volume(b.getVolume())
                        .bs(b.getBs())
                        .time(new Date())
                        .build();
            } else {
                level10.getBuy5()[levelB++] = Quote.builder()
                        .pCode(pCode)
                        .price(BigDecimal.ZERO)
                        .volume(0)
                        .bs(0)
                        .time(new Date())
                        .build();
            }
        }
        System.out.printf("十档行情:%s\n", level10);
    }

    /**
     * 推送成交信息
     *
     * @param type
     * @param b
     * @param s
     * @param dealPrice
     */
    void dealCallback(MatchPool.DealType type, Order b, Order s, BigDecimal dealPrice) {
        if (type == MatchPool.DealType.PART)
            System.out.printf("当前oid=%s,委托价格=%s,成交价格=%s，部分成交，成交量=%s\n", b.getOid(), b.getPrice(), dealPrice, s.getVolume());
        else if (type == MatchPool.DealType.ALL)
            System.out.printf("当前oid=%s,委托价格=%s,成交价格=%s，全部成交，成交量=%s\n", b.getOid(), b.getPrice(), dealPrice, b.getVolume());

    }
}
