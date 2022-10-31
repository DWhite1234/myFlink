package com.cep.functions;

import com.common.beans.Person;
import lombok.extern.slf4j.Slf4j;
import myCep.functions.PatternProcessFunction;
import myCep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @author zt
 */
@Slf4j
public class CepPatternProcessFunction_002 extends PatternProcessFunction<Person, Person> implements TimedOutPartialMatchHandler<Person> {
    private OutputTag<Person> outputTag;

    public CepPatternProcessFunction_002(OutputTag<Person> outputTag) {
        this.outputTag = outputTag;
    }

    @Override
    public void processMatch(Map<String, List<Person>> map, Context context, Collector<Person> collector) throws Exception {
        log.info("匹配到的数据:{}",map.get("msgEvent"));
    }

    @Override
    public void processTimedOutMatch(Map<String, List<Person>> map, Context context) throws Exception {
        log.info("超时的数据:{}",map);
    }
}
