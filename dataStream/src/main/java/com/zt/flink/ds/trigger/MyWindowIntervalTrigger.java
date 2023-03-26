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
public class MyWindowIntervalTrigger extends Trigger<Person, TimeWindow> {
    private ValueStateDescriptor<Long> valueStateDescriptor = new ValueStateDescriptor<Long>("timer", Long.class);



    @Override
    public TriggerResult onElement(Person element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        ValueState<Long> partitionedState = ctx.getPartitionedState(valueStateDescriptor);
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            return TriggerResult.CONTINUE;
        } else {
            if (partitionedState.value()==null) {
                if (Timer.nextTime==0) {
                    Timer.nextTime = System.currentTimeMillis() + 5000;
                }
                ctx.registerProcessingTimeTimer(Timer.nextTime);
                partitionedState.update(Timer.nextTime);
            }
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (ctx.getCurrentWatermark()<window.maxTimestamp()) {
            Timer.nextTime = System.currentTimeMillis()+5000;
            ctx.registerProcessingTimeTimer(Timer.nextTime);
        }
        return TriggerResult.FIRE;
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
