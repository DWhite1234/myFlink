package com.zt.flink.java.utils;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author zt
 */
@Slf4j
public class JsonUtils {

    public static MyJson getJson(String fileName) {
        try {
            InputStream resourceAsStream = JsonUtils.class.getClassLoader().getResourceAsStream(fileName);
            return JSON.parseObject(resourceAsStream, MyJson.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Data
    class MyJson{
        String id;
    }
}
