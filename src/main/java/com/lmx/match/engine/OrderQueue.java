package com.lmx.match.engine;

import it.unimi.dsi.fastutil.objects.ObjectAVLTreeSet;
import it.unimi.dsi.fastutil.objects.ObjectSortedSet;

/**
 * 订单队列
 *
 * @author : lucas
 * @date : 2019/10/26 16:44
 */
public class OrderQueue {

    ObjectSortedSet<Order> buyList = new ObjectAVLTreeSet<>((a, b) -> {
        if (a.getPrice().compareTo(b.getPrice()) == 0) {
            return a.getTime().compareTo(b.getTime());
        } else {
            return -a.getPrice().compareTo(b.getPrice());
        }
    });

    ObjectSortedSet<Order> sellList = new ObjectAVLTreeSet<>((a, b) -> {
        if (a.getPrice().compareTo(b.getPrice()) == 0) {
            return a.getTime().compareTo(b.getTime());
        } else {
            return a.getPrice().compareTo(b.getPrice());
        }
    });

    final Object lock = new Object();//对象级的互斥锁，报单和撮合严格互斥

    boolean checkLimited(Order order) {
        String pCode = order.getPCode();
        Level10 level10 = Level10.Pool.getLevel10(pCode);
        //委托价格大于涨停价或者小于跌停价不能交易
        if ((level10 != null && level10.getUpLimited() != null && level10.getDownLimited() != null)
                && (order.getPrice().compareTo(level10.getUpLimited()) > 0
                || order.getPrice().compareTo(level10.getDownLimited()) < 0)) {
            System.err.printf("当前价格[%s]已经触及涨停或跌停\n", order.getPrice());
            return true;
        }
        return false;
    }

    void addOrder(Order wtOrder) {
        synchronized (lock) {
            if (checkLimited(wtOrder)) {
                return;
            }
            if (wtOrder.getBs() == 0) {
                buyList.add(wtOrder);
            } else {
                sellList.add(wtOrder);
            }
        }
    }


    void revoke(Order wtOrder) {
        synchronized (lock) {
            if (wtOrder.getBs() == 0) {
                buyList.remove(wtOrder);
            } else {
                sellList.remove(wtOrder);
            }
        }
    }
}
