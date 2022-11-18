package com.functions;

import com.common.beans.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zt
 */
@Slf4j
public class Test002 extends KeyedProcessFunction<String, Person,Person> implements CheckpointedFunction {
    private MapStateDescriptor<String, Person> broadDes;
    private BroadcastState<String, Person> state;

    public Test002(MapStateDescriptor<String, Person> broadDes) {
        this.broadDes = broadDes;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        state = context.getOperatorStateStore().getBroadcastState(broadDes);
        log.info("broadcast init:{}",state.entries());
    }

    @Override
    public void processElement(Person value, KeyedProcessFunction<String, Person, Person>.Context ctx, Collector<Person> out) throws Exception {
        log.info("broadstate process:{}",state.entries());
    }
}
