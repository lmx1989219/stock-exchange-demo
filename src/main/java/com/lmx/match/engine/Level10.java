package com.lmx.match.engine;

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
        StringBuilder stringBuilder = new StringBuilder("\n");
        stringBuilder.append("合约=" + pCode).append("\n")
                .append("开盘=" + open).append("\t")
                .append("最新=" + last).append("\t")
                .append("涨停=" + upLimited).append("\t")
                .append("跌停=" + downLimited).append("\n");
        for (Quote quote : sell5) {
            if (quote != null)
                stringBuilder.append(quote.getPrice()).append("\t").append(quote.getVolume()).append("\n");
        }
        Quote s5 = sell5[Math.min(sell5.length - 1, LEVEL - 1)];
        last = s5.getPrice();//卖一档
        stringBuilder.append("-----------").append("\n");
        for (Quote quote : buy5) {
            if (quote != null)
                stringBuilder.append(quote.getPrice()).append("\t").append(quote.getVolume()).append("\n");
        }
        return stringBuilder.append("\n").toString();
    }

    static class Pool {
        public Map<String, Level10> level10Map = Maps.newConcurrentMap();

        public void addPool(String pCode, Level10 level10) {
            level10Map.put(pCode, level10);
        }

        public Level10 getLevel10(String pCode) {
            return level10Map.get(pCode);
        }
    }
}
