package com.zt.flink.java.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * @author zt
 */
@Slf4j
public class ThreadPoolConfig {
    //线程池
    private static ThreadPoolExecutor threadPoolExecutor;

    public static synchronized ThreadPoolExecutor initThreadPool() {
        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolConfig.class) {
                if (threadPoolExecutor == null) {
                    try {
                        threadPoolExecutor = new ThreadPoolExecutor(
                                3,
                                3,
                                60000,
                                TimeUnit.MILLISECONDS,
                                new ArrayBlockingQueue<>(1),
                                new ThreadPoolExecutor.DiscardOldestPolicy()
                        );

                        return threadPoolExecutor;
                    }catch (Exception e) {
                        log.error("线程池创建失败....",e);
                    }
                }
            }
        }
        return threadPoolExecutor;
    }

    //线程池销毁
    public static void cancel() {
        threadPoolExecutor.shutdownNow();
        threadPoolExecutor = null;
    }
}
