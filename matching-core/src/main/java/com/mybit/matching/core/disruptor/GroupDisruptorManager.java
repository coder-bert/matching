package com.mybit.matching.core.disruptor;

import com.lmax.disruptor.dsl.Disruptor;
import com.mybit.matching.core.InitializeAndCloseable;
import com.mybit.matching.core.config.Config;
import com.mybit.matching.core.entity.CloseDumpEvent;
import com.mybit.matching.core.entity.DumpEvent;
import com.mybit.matching.core.entity.Event;
import com.mybit.matching.core.entity.RegularDumpEvent;
import com.mybit.matching.core.orderbook.BaseEventHandler;
import com.mybit.matching.core.orderbook.OrderBookManager;
import com.mybit.matching.core.utils.AssertUtil;
import com.mybit.matching.core.utils.Utils;
import com.mybit.matching.core.utils.threads.ExtExecutors;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Data
public class GroupDisruptorManager implements InitializeAndCloseable {

    private final Config config = Config.getInstance();
    private final OrderBookManager orderBookManager = OrderBookManager.getInstance();

    private final Map<String, GroupDisruptor> groupDisruptorMap = new ConcurrentHashMap<>(1024);

    private volatile boolean isRunning = false;     // 关闭时设置，不再允许外部fire events

    // 定时发送dumpEvent
    private ScheduledExecutorService scheduledExecutorService;
    private ScheduledFuture<?> scheduledFuture;
    private AtomicLong lastDumpEventTime = new AtomicLong(0);
    private volatile CloseDumpEvent hasCloseDumpEvent = null;


    private static volatile GroupDisruptorManager instance;

    private GroupDisruptorManager() {
    }

    public static GroupDisruptorManager getInstance() {
        if (instance == null) {
            synchronized (GroupDisruptorManager.class) {
                if (instance == null) {
                    instance = new GroupDisruptorManager();
                }
            }
        }
        return instance;
    }


    @Override
    public boolean init() {
        return true;
    }

    @Override
    public boolean startup() {
        if (isRunning) return false;
        isRunning = true;

        // 定时触发 常规DUMP事件
        startRegularDumpTask();

        return true;
    }

    @Override
    public boolean close() {
        if (!isRunning) return false;
        isRunning = false;

        sendCloseDumpEvent();

        return true;
    }

    /**
     * 定时间隔进行常规dump
     */
    public void startRegularDumpTask() {
        long minDumpInterval = Math.max(Config.getInstance().getMinDumpInterval(), 5);  // 单位秒
        long dumpInterval = Math.max(Config.getInstance().getDumpInterval(), 10);       // 单位秒
        dumpInterval = Math.max(minDumpInterval, dumpInterval);

        scheduledExecutorService = ExtExecutors.newSingleThreadScheduledExecutor();
        scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            if (isRunning && lastDumpEventTime.get() + minDumpInterval * 1000 <= now) {  // 限定最小dump时间间隔，避免太频繁
                lastDumpEventTime.set(now);

                Set<String> groupKeys = groupDisruptorMap.keySet();
                if (groupKeys.isEmpty() || !isRunning) return;

                RegularDumpEvent event = new RegularDumpEvent(config.getDumpPath(), now, groupKeys.size());
                groupDisruptorMap.forEach((x, groupDisruptor) -> groupDisruptor.fireRegularDumpEvent(event));
            }
        }, dumpInterval, dumpInterval, TimeUnit.SECONDS);
    }

    public void sendCloseDumpEvent() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }

        // 向各disruptor发送ExitEvent，并停止fireEvent
        // handler接收到ExitEvent后
        Set<String> groupKeys = groupDisruptorMap.keySet();
        if (groupKeys.isEmpty()) {
            log.info("no processing data threads, skip closeDump");
            return;
        }

        int timeout = config.getWaitingCloseTimeout();
        CloseDumpEvent closeDumpEvent = new CloseDumpEvent(config.getDumpPath(), System.currentTimeMillis(), groupKeys.size(), timeout);
        this.hasCloseDumpEvent = closeDumpEvent;
        log.info("fire CloseDumpEvent @{} to {}", System.identityHashCode(closeDumpEvent), groupDisruptorMap.keySet());
        groupDisruptorMap.forEach((x, disruptor) -> disruptor.closeAndFireDumpEvent(closeDumpEvent));    // 停止发送普通事件，触发CloseDump事件

        try {
            // 等待DUMP完成，再退出
            log.info("waiting for dump closeLatch ...");
            long startTime = System.currentTimeMillis();
            closeDumpEvent.getCloseLatch().await();
//            if (closeDumpEvent.getCloseLatch().await(timeout + 1, TimeUnit.MILLISECONDS)) {
//                log.info("waiting for dump closeLatch finish {}ms", System.currentTimeMillis() - startTime);
//            } else {
//                log.error("waiting for dump closeLatch expired {}ms", timeout);
//            }
        } catch (InterruptedException e) {
            log.error("InterruptedException", e);
        }
    }

    public DumpEvent hasCloseDumpEvent() {
        return hasCloseDumpEvent;
    }



    public GroupDisruptor newDisruptor(String groupKey, TopicPartition tp) {
        AssertUtil.notEmpty(groupKey, "groupKey should not be null or empty");
        synchronized (GroupDisruptorManager.class) {
            GroupDisruptor groupDisruptor = groupDisruptorMap.get(groupKey);
            if (groupDisruptor != null) {
                return groupDisruptor;
            }
            return newDisruptor(groupKey, tp, Utils.nearestPowerOfTwo(config.getBufferSize()));
        }
    }

    private GroupDisruptor newDisruptor(String groupKey, TopicPartition tp, int bufferSize) {
        AssertUtil.notEmpty(groupKey, "groupKey should not be null or empty");

        log.info("newDisruptor {} {}", groupKey, bufferSize);
        BaseEventHandler<Event> matchHandler = DisruptorFactory.newHandler(groupKey, tp);
        Disruptor<DataEvent> disruptor = DisruptorFactory.newDisruptor(groupKey, bufferSize, matchHandler);
        GroupDisruptor groupDisruptor = new GroupDisruptor(groupKey, disruptor, matchHandler);

        groupDisruptorMap.put(groupKey, groupDisruptor);

        return groupDisruptor;
    }

    public GroupDisruptor getDisruptorOrCreate(String groupKey, TopicPartition tp) {
        AssertUtil.notEmpty(groupKey, "key invalid");
        GroupDisruptor groupDisruptor = groupDisruptorMap.get(groupKey);
        if (groupDisruptor != null) {
            return groupDisruptor;
        }
        groupDisruptor = newDisruptor(groupKey, tp);
        groupDisruptor.init();
        groupDisruptor.startup();
        return groupDisruptor;
    }



    /**
     * for test only
     */
    public boolean isDiscard(String groupKey, long offset) {
        boolean ret = false;
        GroupDisruptor disruptor = groupDisruptorMap.get(groupKey);
        if (disruptor != null) {
            ret = disruptor.isDiscard(groupKey, offset);
            if (ret) return true;
        } /*else if (!groupKey.isEmpty()) {
            // disruptor==null 可能判断是的旧数据（重启后内存没有记录丢弃的offsets）
            return true;
        }*/

        for (Map.Entry<String, GroupDisruptor> entry : groupDisruptorMap.entrySet()) {
            ret = entry.getValue().isDiscard(entry.getKey(), offset);
            if (ret) return true;
        }

        long minOffset = -1;
        for (Map.Entry<String, GroupDisruptor> entry : groupDisruptorMap.entrySet()) {
            long offset1 = entry.getValue().fistDiscardOffset(entry.getKey(), offset);
            minOffset = Math.min(minOffset, offset1);
        }
        if (offset < minOffset)
            return true;

        return false;
    }

}
