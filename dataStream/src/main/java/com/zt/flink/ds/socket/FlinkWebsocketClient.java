package com.zt.flink.ds.socket;

import lombok.extern.slf4j.Slf4j;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;

/**
 * @author zt
 */
@Slf4j
public class FlinkWebsocketClient extends WebSocketClient {
    private String wsName;

    public FlinkWebsocketClient(URI serverUri, String wsName) {
        super(serverUri);
        this.wsName = wsName;
    }

    @Override
    public void onOpen(ServerHandshake serverHandshake) {
        log.info("[websocket {}] Websocket客户端连接成功",wsName);
    }

    @Override
    public void onMessage(String msg) {
        log.info("[websocket {}] 收到消息：{}",wsName,msg);
    }

    @Override
    public void onClose(int i, String reason, boolean b) {
        log.info("[websocket {}] Websocket客户端关闭,原因:{}",wsName,reason);
    }

    @Override
    public void onError(Exception e) {
        log.info("[websocket {}] Websocket客户端出现异常, 异常原因为：{}",wsName,e.getMessage());
    }
}
