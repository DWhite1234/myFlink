package com.functions;

import com.common.beans.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

/**
 * @author zt
 */
@Slf4j
public class Test003 extends RichMapFunction<Person,Person> implements CheckpointedFunction {
    private ListState<Person> listState;
    private ListStateDescriptor<Person> listStateDescriptor;

    public Test003(ListStateDescriptor<Person> listStateDescriptor) {
        this.listStateDescriptor = listStateDescriptor;
    }

    @Override
    public Person map(Person value) throws Exception {
        log.info("map listState:{}",listState.get());
        return value;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        listStateDescriptor = new ListStateDescriptor<>("list", Person.class);
        listState = context.getOperatorStateStore().getUnionListState(listStateDescriptor);
        log.info("map init:{}",listState.get());
    }
}
