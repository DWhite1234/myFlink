package com.zt.flink.ds.io;

import com.common.beans.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author zt
 */
@Slf4j
public class Test001 extends ProcessWindowFunction<Person,Person, String, TimeWindow> implements CheckpointedFunction {
    private ListState<Person> listState;
    private ListStateDescriptor<Person> listStateDescriptor;

    public Test001(ListStateDescriptor<Person> listStateDescriptor) {
        this.listStateDescriptor = listStateDescriptor;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        log.info("执行ck........");
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        log.info("initState........");
        listStateDescriptor = new ListStateDescriptor<>("listState", Person.class);
//        listState = context.getOperatorStateStore().getListState(listStateDescriptor);
        listState = context.getOperatorStateStore().getUnionListState(listStateDescriptor);
        log.info("initState listState:{}",listState.get());
//        log.info("initState unionListState:{}",unionListState);
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<Person> listStateDescriptor = new ListStateDescriptor<>("listState", Person.class);
    }


    @Override
    public void process(String s, ProcessWindowFunction<Person, Person, String, TimeWindow>.Context context, Iterable<Person> elements, Collector<Person> out) throws Exception {
        for (Person element : elements) {
            listState.add(element);
//            unionListState.add(element);
            out.collect(element);
        }
        log.info("window:{},thread:{},listState:{}", context.window().getStart(),Thread.currentThread().getId(), listState.get());
    }
}
