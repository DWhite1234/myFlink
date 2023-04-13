package com.zt.flink.ds.Json_to_ds;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zt.flink.ds.bean.MyJson;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.InputStream;

/**
 * @author zt
 */
public class JsonToDs {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameterTool);
        MyJson myJson = null;
        try {
            InputStream resourceAsStream = JsonToDs.class.getClassLoader().getResourceAsStream("demo2.json");
            myJson = JSON.parseObject(resourceAsStream, MyJson.class);
            System.out.println(myJson);
        } catch (Exception e) {
            e.printStackTrace();
        }

        SingleOutputStreamOperator<JSONObject> source = env.socketTextStream("localhost", 999)
                .map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> dataStream = DsUtils.getDataStream(myJson, source);
        dataStream.print();

        env.execute();
    }
}
