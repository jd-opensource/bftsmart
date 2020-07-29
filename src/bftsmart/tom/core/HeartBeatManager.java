package bftsmart.tom.core;

import bftsmart.consensus.messages.HeartBeatMessage;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HeartBeatManager {

    private static final long HEART_BEAT_PERIOD = 10000;

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private final TOMLayer tomLayer;

    public HeartBeatManager(TOMLayer tomLayer) {
        this.tomLayer = tomLayer;
    }

    /**
     * 启动心跳处理器
     *
     */
    public void start() {
        executor.scheduleAtFixedRate(() -> {
            try {
                // 生成心跳信息，发送给其他节点
                HeartBeatMessage heartBeatMessage = newHeartBeatMessage();
                tomLayer.getCommunication().send(tomLayer.controller.getCurrentViewOtherAcceptors(), heartBeatMessage);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, HEART_BEAT_PERIOD, HEART_BEAT_PERIOD, TimeUnit.MILLISECONDS);
    }

    private HeartBeatMessage newHeartBeatMessage() {
        return new HeartBeatMessage(tomLayer.controller.getStaticConf().getProcessId(),
                tomLayer.execManager.getCurrentLeader());
    }
}
