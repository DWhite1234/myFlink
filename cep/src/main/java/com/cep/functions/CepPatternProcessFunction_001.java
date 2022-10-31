package com.cep.functions;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @author zt
 */
@Slf4j
public class CepPatternProcessFunction_001 extends PatternProcessFunction<JSONObject, JSONObject> implements TimedOutPartialMatchHandler<JSONObject> {
    private OutputTag<JSONObject> outputTag;

    public CepPatternProcessFunction_001(OutputTag<JSONObject> outputTag) {
        this.outputTag = outputTag;
    }

    @Override
    public void processMatch(Map<String, List<JSONObject>> map, Context context, Collector<JSONObject> collector) throws Exception {
        log.info("匹配到的数据:{}",map.get("msgEvent"));
    }

    @Override
    public void processTimedOutMatch(Map<String, List<JSONObject>> map, Context context) throws Exception {
        log.info("超时的数据:{}",map);
    }
}
