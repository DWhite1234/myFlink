package com.zt.flink.ds.io;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.ArrayList;

/**
 * @author zt
 */

@Slf4j
public class AsyncFunction extends RichAsyncFunction<String, String> {
    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
      log.info("输入数据:{}",input);
      ArrayList<String> list = new ArrayList<>();
        list.add(input + "-" + 0);
        list.add(input + "-" + 1);
        list.add(input + "-" + 2);
        log.info("输出数据:{}",list);
      resultFuture.complete(list);

    }
}
