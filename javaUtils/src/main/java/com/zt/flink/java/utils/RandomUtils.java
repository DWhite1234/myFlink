package com.zt.flink.java.utils;

/**
 * @author zt
 */

public class RandomUtils {
    //[start,end)
    public static int getRange(int start,int end) {
        return (int)Math.floor(Math.random()*(end-start)+start);
    }
}
