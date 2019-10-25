package com.lmx.match.engine;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;

/**
 * 行情
 *
 * @author: lucas
 * @create: 2019-10-24 13:28
 **/
@Data
@Builder
public class Quote {
    private String pCode;
    private Integer bs;
    private BigDecimal price;
    private Integer volume;
    private Date time;
}
