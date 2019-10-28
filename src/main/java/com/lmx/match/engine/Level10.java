package com.lmx.match.engine;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Data;

import java.math.BigDecimal;
import java.util.Map;

/**
 * 10档行情
 *
 * @author: lucas
 * @create: 2019-10-24 13:28
 **/
@Data
public class Level10 {
    public final static Integer LEVEL = 5;
    private String pCode;
    private Quote[] buy5 = new Quote[LEVEL];
    private Quote[] sell5 = new Quote[LEVEL];
    /*最新价=卖一价*/
    private BigDecimal last = BigDecimal.ZERO;
    private BigDecimal open, upLimited, downLimited = BigDecimal.ZERO;


    public String toString() {
        Integer totalBuy = Lists.newArrayList(buy5).stream().map(Quote::getVolume).reduce(0, Integer::sum);
        Integer totalSell = Lists.newArrayList(sell5).stream().map(Quote::getVolume).reduce(0, Integer::sum);
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("合约=" + pCode).append("\n")
                .append("开盘=" + open).append("\t")
                .append("最新=" + last).append("\t")
                .append("涨停=" + upLimited).append("\t")
                .append("跌停=" + downLimited).append("\t")
                .append("总买=" + totalBuy).append("\t")
                .append("总卖=" + totalSell).append("\n");
        int sLe = LEVEL;
        for (int i = 0; i < LEVEL; i++) {
            Quote quote = sell5[i];
            if (quote != null)
                stringBuilder.append("卖").append(sLe--).append("\t").append(quote.getPrice()).append("\t").append(quote.getVolume()).append("\n");
            else//为0说明没有符合条件的卖单，排在队列后面
                stringBuilder.append("卖").append(sLe--).append("\t").append(0.00d).append("\t").append(0).append("\n");
        }
        Quote s5 = sell5[Math.min(sell5.length - 1, LEVEL - 1)];
        last = s5.getPrice();//卖一档
        stringBuilder.append("---------------").append("\n");
        int bLe = 1;
        for (int i = 0; i < LEVEL; i++) {
            Quote quote = buy5[i];
            if (quote != null)
                stringBuilder.append("买").append(bLe++).append("\t").append(quote.getPrice()).append("\t").append(quote.getVolume()).append("\n");
            else//为0说明没有符合条件的买单，排在队列后面
                stringBuilder.append("买").append(bLe++).append("\t").append(0.00d).append("\t").append(0).append("\n");
        }
        return stringBuilder.append("\n").toString();
    }

    static class Pool {
        static public Map<String, Level10> level10Map = Maps.newConcurrentMap();

        static public void addPool(String pCode, Level10 level10) {
            level10Map.put(pCode, level10);
        }

        static public Level10 getLevel10(String pCode) {
            return level10Map.get(pCode);
        }
    }
}
