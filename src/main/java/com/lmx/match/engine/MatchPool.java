package com.lmx.match.engine;

import com.google.common.collect.Maps;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    public void initPool(String pCode, OrderQueue queue) {
        queueMap.put(pCode, queue);
    }

    public OrderQueue getQueue(String pCode) {
        return queueMap.get(pCode);
    }

    Level10 initQuote(String pCode, BigDecimal dealPrice) {
        //行情为空说明是首次集中竞价，取卖价最低的为成交价并定为开盘价，同时设置涨跌停
        if (Level10.Pool.getLevel10(pCode) == null) {
            Level10 level10 = new Level10();
            level10.setPCode(pCode);
            level10.setOpen(dealPrice);
            level10.setUpLimited(dealPrice.multiply(new BigDecimal(1 + 0.1)).setScale(2, RoundingMode.HALF_UP));
            level10.setDownLimited(dealPrice.multiply(new BigDecimal(1 - 0.1)).setScale(2, RoundingMode.HALF_UP));
            Level10.Pool.addPool(pCode, level10);
            return level10;
        } else {
            return Level10.Pool.getLevel10(pCode);
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
//                        System.out.printf("买队列=%s\n卖队列=%s\n", buyList, sellList);
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
                                //委买价大于等于成交价，并且委卖价大于成交价时，取委卖价为成交价
                                if (s.getPrice().compareTo(dealPrice) >= 0) {
                                    dealPrice = s.getPrice();
                                }
                                //发布行情事件
                                QuoteGenerator.systemBus.publishEvent(new MatchEventDispatcher.QuoteEvent(queue, b.getPCode(), dealPrice, MatchEventDispatcher.EventEnum.PUB_QUOTE));
                                //单笔买单量小于等于卖单，则直接成交
                                if (b.getVolume().compareTo(s.getVolume()) <= 0) {
                                    QuoteGenerator.systemBus.publishEvent(new MatchEventDispatcher.DealEvent(DealType.ALL, b, s, dealPrice, MatchEventDispatcher.EventEnum.DEAL_CALLBACK));
                                    buyIt.remove();
                                    s.setVolume(s.getVolume() - b.getVolume());
                                    //卖单量为0则移除队列
                                    if (s.getVolume() == 0) {
                                        sellIt.remove();
                                    }
                                    //进行下一个买单撮合
                                    break;
                                } else {//单笔买单量大于卖单，依次部分成交
                                    QuoteGenerator.systemBus.publishEvent(new MatchEventDispatcher.DealEvent(DealType.PART, b, s, dealPrice, MatchEventDispatcher.EventEnum.DEAL_CALLBACK));
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

}
