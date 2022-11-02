package com.spring.ds;

import com.alibaba.fastjson.JSONObject;
import com.spring.utils.SpelUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zt
 */
@Slf4j
public class DS_001 extends KeyedProcessFunction<String, JSONObject, JSONObject> implements CheckpointedFunction {
    private MapState<String, JSONObject> mapState;
    private boolean flag = true;
    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, JSONObject> mapStateDescriptor = new MapStateDescriptor<>("map",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<JSONObject>() {
                }));

        mapState = getRuntimeContext().getMapState(mapStateDescriptor);
    }
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListState<MapState<String, JSONObject>> unionList = context.getOperatorStateStore().getUnionListState(new ListStateDescriptor<MapState<String, JSONObject>>("unionList",
                TypeInformation.of(new TypeHint<MapState<String, JSONObject>>() {
                })));

        for (MapState<String, JSONObject> state : unionList.get()) {
            state.entries();
        }
    }

    @Override
    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        mapState.put(value.getString("name")+"-"+value.getInteger("age"),value);
        log.info("第一次 el中的获取的state:{}",SpelUtil.getState("msg").entries());
        if (SpelUtil.getState(ctx.getCurrentKey())==null) {
            SpelUtil.setContextVariable(ctx.getCurrentKey(),mapState);
        }
        log.info("当前key:{},当前状态:{},线程号:{}",ctx.getCurrentKey(),mapState.entries(),Thread.currentThread().getId());
        log.info("第二次 el中的获取的state:{}",SpelUtil.getState(ctx.getCurrentKey()).entries());
    }
}
