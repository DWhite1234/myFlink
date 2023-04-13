package com.zt.flink.ds.io;

import com.common.beans.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;

/**
 * @author zt
 */
/*
总结:异步io中不能使用状态
 */
@Slf4j
public class AsyncFunction extends RichAsyncFunction<Person, Person> {
    private MapState<String, String> mapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, String> descriptor = new MapStateDescriptor<>("map", String.class, String.class);
        mapState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void asyncInvoke(Person input, ResultFuture<Person> resultFuture) throws Exception {
        mapState.put("1", "1");
        resultFuture.complete(Collections.singletonList(input));
        log.info("map:{}",mapState.entries());
    }
}
