package com.functions;

import com.common.beans.Person;
import com.sun.org.apache.bcel.internal.generic.NEW;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author zt
 */
@Slf4j
public class Test001 extends ProcessWindowFunction<Person,Person, String, TimeWindow> implements CheckpointedFunction {
    private ListState<Person> listState;
    private ListState<Person> listState2;
    private ListState<Person> unionListState;
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        log.info("执行ck........");
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        log.info("initState........");
        ListStateDescriptor<Person> listStateDescriptor = new ListStateDescriptor<>("listState", Person.class);
        listState = context.getOperatorStateStore().getListState(listStateDescriptor);
        log.info("initState listState:{}",listState.get());
//        unionListState = context.getOperatorStateStore().getUnionListState(listStateDescriptor);
//        log.info("initState unionListState:{}",unionListState);
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<Person> listStateDescriptor = new ListStateDescriptor<>("listState", Person.class);
        listState2 = getRuntimeContext().getListState(listStateDescriptor);
        log.info("open listState:{}",listState2);
//        log.info("open unionListState:{}",unionListState);
    }


    @Override
    public void process(String s, ProcessWindowFunction<Person, Person, String, TimeWindow>.Context context, Iterable<Person> elements, Collector<Person> out) throws Exception {
        for (Person element : elements) {
            listState.add(element);
            listState2.add(element);
//            unionListState.add(element);
        }
        log.info("window:{},listState:{}", context.window().getStart(), listState.get());
        log.info("window:{},listState2:{}", context.window().getStart(), listState2.get());
    }
}
