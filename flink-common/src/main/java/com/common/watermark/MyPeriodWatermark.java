package com.common.watermark;

import com.alibaba.fastjson.JSONObject;
import com.common.beans.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.util.Preconditions;

import java.time.Duration;

/**
 * @author zt
 */
@Slf4j
public class MyPeriodWatermark implements WatermarkGenerator<JSONObject> {

    private long maxTimestamp;
    private final long outOfOrdernessMillis;

    public MyPeriodWatermark(Duration maxOutOfOrderness) {
        Preconditions.checkNotNull(maxOutOfOrderness, "maxOutOfOrderness");
        Preconditions.checkArgument(!maxOutOfOrderness.isNegative(), "maxOutOfOrderness cannot be negative");
        this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();
        this.maxTimestamp = Long.MIN_VALUE + this.outOfOrdernessMillis + 1L;
    }

    @Override
    public void onEvent(JSONObject event, long eventTimestamp, WatermarkOutput watermarkOutput) {
        this.maxTimestamp = Math.max(this.maxTimestamp, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
        this.maxTimestamp += 1;
//        log.info("开始生成水印,当前时间:{}",this.maxTimestamp);
        watermarkOutput.emitWatermark(new Watermark(this.maxTimestamp - this.outOfOrdernessMillis - 1L));
    }
}
