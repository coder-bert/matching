package com.mybit.matching.core.dump;

import com.mybit.matching.core.entity.OrderEvent;

public interface LoadCallback {
    void onData(OrderEvent event);
}
