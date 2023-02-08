package com.zt.flink.ds.trigger;

import com.common.beans.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author zt
 */
@Slf4j
public class MyIntervalTrigger extends Trigger<Person, TimeWindow> {
    private ValueStateDescriptor<Long> valueStateDescriptor = new ValueStateDescriptor<Long>("timer", Long.class);
    @Override
    public TriggerResult onElement(Person element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        ValueState<Long> partitionedState = ctx.getPartitionedState(valueStateDescriptor);
        log.info("当前wm:{},当前maxTimestamp:{}", ctx.getCurrentWatermark(),window.maxTimestamp());
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            return TriggerResult.FIRE;
        } else {
            if (partitionedState.value()==null) {
                Long newTime = System.currentTimeMillis()+5000;
                ctx.registerProcessingTimeTimer(newTime);
                partitionedState.update(newTime);
            }
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (ctx.getCurrentWatermark()<window.maxTimestamp()) {
            Long newTime = System.currentTimeMillis()+5000;
            log.info("当前wm:{},窗口:{},maxTimestamp:{}",ctx.getCurrentWatermark(),window.getStart(),window.maxTimestamp());
            ctx.registerProcessingTimeTimer(newTime);
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
//        ValueState<Long> partitionedState = ctx.getPartitionedState(valueStateDescriptor);
//        ctx.deleteProcessingTimeTimer(partitionedState.value());
//        partitionedState.clear();
        return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }
}
