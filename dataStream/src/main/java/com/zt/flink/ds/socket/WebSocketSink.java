package com.zt.flink.ds.socket;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zt
 */
@Slf4j
public class WebSocketSink extends RichSinkFunction<String> {
    private MapState<String, List<String>> mapState;
    private String socketUrl = "ws://172.1.20.156:8080/metrics/flinkData/002";
    private Integer retryTimes = 3;
    private Integer heartBeatInterval =3000;
    private String wsName;
    private FlinkWebsocketClient websocketClient;

    @Override
    public void open(Configuration parameters) throws Exception {
        wsName = "Flink-"+getRuntimeContext().getNumberOfParallelSubtasks();
        websocketClient = new FlinkWebsocketClient(new URI(socketUrl), wsName);
        websocketClient.connect();
        websocketClient.setConnectionLostTimeout(0);
        MapStateDescriptor<String, List<String>> mapStateDescriptor = new MapStateDescriptor<>("map",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<List<String>>() {
                }));

        mapState = getRuntimeContext().getMapState(mapStateDescriptor);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
//        boolean isAlive = checkSocketConn();
        boolean isAlive = true;
        if (isAlive) {
            //连接存活,数据发送
            websocketClient.send(value);
        }else{
            //连接失败,保存数据
            List<String> strings = mapState.get(value);
            if (strings==null) {
                strings = new ArrayList<>();
                mapState.put(value,strings);
            }
            strings.add(value);
            log.info("{}",mapState.entries());
        }
    }

    public boolean checkSocketConn() {
        AtomicBoolean isAlive = new AtomicBoolean(false);
        new Thread(()->{
            int retry = retryTimes;
            while (retry >= 0) {
                try {
                    Thread.sleep(heartBeatInterval);
                    websocketClient.send("[websocket " + wsName + "] 心跳检测");
                    isAlive.set(true);
                    break;
                } catch (Exception e) {
                    log.error("[websocket {}] 发生异常{}", wsName, e.getMessage());
                    try {
                        log.info("[websocket {}] {}ms后尝试第{}次重新连接",wsName,heartBeatInterval,3-retry);
                        websocketClient.reconnect();
                        websocketClient.setConnectionLostTimeout(0);
                        retry--;
                    } catch (Exception ex) {
                        log.error("[websocket {}] 重连异常,{}", wsName, ex.getMessage());
                    }
                }
            }
        }).start();
        return isAlive.get();
    }
}
