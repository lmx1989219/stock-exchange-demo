package com.lmx.match.engine;

import com.google.common.eventbus.EventBus;

/**
 * 系统总线
 *
 * @author: lucas
 * @create: 2019-10-28 09:44
 **/
public class SystemBus {
    EventBus eventBus = new EventBus();

    public void registryEvent(MatchEventDispatcher matchEventDispatcher) {
        eventBus.register(matchEventDispatcher);
    }

    public void publishEvent(Object object) {
        eventBus.post(object);
    }
}
