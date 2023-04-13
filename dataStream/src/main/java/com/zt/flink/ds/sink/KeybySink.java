package com.zt.flink.ds.sink;

import com.common.beans.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author zt
 */
@Slf4j
public class KeybySink extends RichSinkFunction<Person> {

    @Override
    public void invoke(Person value, Context context) throws Exception {
        log.info("value:{} subtask:{}",value,getRuntimeContext().getIndexOfThisSubtask());
    }
}
