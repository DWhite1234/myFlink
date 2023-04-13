package com.zt.flink.ds.Json_to_ds;

import com.alibaba.fastjson.JSONObject;
import com.zt.flink.ds.bean.MyJson;
import com.zt.flink.ds.bean.Operator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @author zt
 */
public class DsUtils {
    public static SingleOutputStreamOperator<JSONObject> getDataStream(MyJson json,SingleOutputStreamOperator<JSONObject> source) {
        List<Operator> operators = json.getOperators();
        for (Operator operator : operators) {
            String id = operator.getId();
            source = organizeOperator(id, source);

        }
        return source;
    }

    public static SingleOutputStreamOperator<JSONObject> organizeOperator(String id,DataStream<JSONObject> dataStream) {
        SingleOutputStreamOperator<JSONObject> ds = null;
        switch (id) {
            case "1":
                ds = map(dataStream);
                break;
            case "2":
                ds = keyByProcess(dataStream);
                break;
            case "3":
                ds = process(dataStream);
                break;
            default:
                break;
        }
        return ds;
    }

    public static SingleOutputStreamOperator<JSONObject> map(DataStream<JSONObject> dataStream) {
        return dataStream.map(data->{
            System.out.println("当前是map算子");
            return data;
        });
    }

    public static SingleOutputStreamOperator<JSONObject> keyByProcess(DataStream<JSONObject> dataStream) {
        return dataStream.keyBy(data->data.getString("id"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        System.out.println("当前是keyByProcess算子");
                        out.collect(value);
                    }
                });
    }

    public static SingleOutputStreamOperator<JSONObject> process(DataStream<JSONObject> dataStream) {
        return dataStream.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                System.out.println("当前是process算子");
                out.collect(value);
            }
        });
    }
}
