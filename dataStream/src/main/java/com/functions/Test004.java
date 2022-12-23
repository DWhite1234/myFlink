package com.functions;

import com.common.beans.Person;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zt
 */

public class Test004 extends RichMapFunction<String,String>{
    private static Map<String, Person> map= new HashMap<>();;

    @Override
    public String map(String value) throws Exception {
        map.put("1", new Person());
        return value;
    }
}