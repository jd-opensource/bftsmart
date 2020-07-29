package bftsmart.tom.core;

import bftsmart.consensus.messages.HeartBeatMessage;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HeartBeatManager {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(HeartBeatManager.class);

    private static final long HEART_BEAT_PERIOD = 10000L;

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
                LOGGER.info("Send heart beat message to other servers !");
            } catch (Exception e) {
                LOGGER.error("Send heart beat message error !!!", e);
            }
        }, HEART_BEAT_PERIOD, HEART_BEAT_PERIOD, TimeUnit.MILLISECONDS);
    }

    private HeartBeatMessage newHeartBeatMessage() {
        return new HeartBeatMessage(tomLayer.controller.getStaticConf().getProcessId(),
                tomLayer.execManager.getCurrentLeader());
    }
}
