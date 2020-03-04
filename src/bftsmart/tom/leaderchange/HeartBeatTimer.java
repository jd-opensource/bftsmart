package bftsmart.tom.leaderchange;

import bftsmart.communication.ServerCommunicationSystem;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.tom.core.TOMLayer;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This thread serves as a manager for all timers of pending requests.
 *
 */
public class HeartBeatTimer {

    private Timer leaderTimer = new Timer("heart beat leader timer");

    private Timer replicaTimer = new Timer("heart beat replica timer");

    private RequestsTimer requestsTimer;

    private TOMLayer tomLayer; // TOM layer

    private volatile InnerHeartBeatMessage innerHeartBeatMessage;

    private Lock hbLock = new ReentrantLock();

    private ServerCommunicationSystem communication; // Communication system between replicas

    private ServerViewController controller; // Reconfiguration manager

    private long heartBeatPeriod;

    private long heartBeatTimeout;

    /**
     * Creates a new instance of RequestsTimer
     * @param tomLayer TOM layer
     */
    public HeartBeatTimer(TOMLayer tomLayer, ServerCommunicationSystem communication, ServerViewController controller, RequestsTimer requestsTimer) {
        this.tomLayer = tomLayer;
        
        this.communication = communication;

        this.controller = controller;

        this.requestsTimer = requestsTimer;

        this.heartBeatPeriod = this.controller.getStaticConf().getHeartBeatPeriod();

        this.heartBeatTimeout = this.controller.getStaticConf().getHeartBeatTimeout();
    }

    public void start() {
        if (tomLayer.isLeader()) {
            leaderTimerStart();
        } else {
            replicaTimerStart();
        }
    }

    public void restart() {
        stopAll();
        start();
    }

    public void leaderTimerStart() {
        // stop Replica timer，and start leader timer
        if (leaderTimer == null) {
            leaderTimer = new Timer("heart beat leader timer");
        }
        leaderTimer.scheduleAtFixedRate(new LeaderTimerTask(), 0, heartBeatPeriod);
    }

    public void replicaTimerStart() {
        if (replicaTimer == null) {
            replicaTimer = new Timer("heart beat replica timer");
        }
        replicaTimer.scheduleAtFixedRate(new ReplicaTimerTask(), heartBeatTimeout, heartBeatTimeout);
    }

    public void stopAll() {
        if (replicaTimer != null) {
            replicaTimer.cancel();
        }
        if (leaderTimer != null) {
            leaderTimer.cancel();
        }
        replicaTimer = null;
        leaderTimer = null;
    }

    /**
     * 收到心跳消息
     * @param heartBeatMessage
     */
    public void receiveHeartBeatMessage(HeartBeatMessage heartBeatMessage) {
        hbLock.lock();
        try {
            if (heartBeatMessage.getLeader() == tomLayer.leader()) {
                System.out.printf("node %s receive heart beat from %s \r\n",
                        this.controller.getStaticConf().getProcessId(), heartBeatMessage.getLeader());
                innerHeartBeatMessage = new InnerHeartBeatMessage(System.currentTimeMillis(), heartBeatMessage);
            }
        } finally {
            hbLock.unlock();
        }
    }

    /**
     *
     */
    class LeaderTimerTask extends TimerTask {

        @Override
        /**
         * This is the code for the TimerTask. It executes the timeout for the first
         * message on the watched list.
         */
        public void run() {
            // 再次判断是否是Leader
            if (tomLayer.isLeader()) {
                // 如果是Leader则发送心跳信息给其他节点，当前节点除外
                HeartBeatMessage heartBeatMessage = new HeartBeatMessage(controller.getStaticConf().getProcessId(),
                        controller.getStaticConf().getProcessId());
                communication.send(controller.getCurrentViewOtherAcceptors(), heartBeatMessage);
            }
        }
    }

    class ReplicaTimerTask extends TimerTask {

        @Override
        public void run() {
            // 再次判断是否是Leader
            if (!tomLayer.isLeader()) {
                // 检查收到的InnerHeartBeatMessage是否超时
                hbLock.lock();
                try {
                    System.out.printf("node %s check heart beat message \r\n", controller.getStaticConf().getProcessId());
                    if (innerHeartBeatMessage == null) {
                        // todo 此处触发超时
                        if (requestsTimer != null) {
                            requestsTimer.run_lc_protocol();
                        }
                    } else {
                        // 判断时间
                        long lastTime = innerHeartBeatMessage.getTime();
                        if (System.currentTimeMillis() - lastTime > heartBeatTimeout) {
                            // todo 此处触发超时
                            if (requestsTimer != null) {
                                requestsTimer.run_lc_protocol();
                            }
                        }
                    }
                } finally {
                    hbLock.unlock();
                }
            }
        }
    }
    
    class InnerHeartBeatMessage {

        private long time;

        private HeartBeatMessage heartBeatMessage;

        public InnerHeartBeatMessage(long time, HeartBeatMessage heartBeatMessage) {
            this.time = time;
            this.heartBeatMessage = heartBeatMessage;
        }

        public long getTime() {
            return time;
        }

        public HeartBeatMessage getHeartBeatMessage() {
            return heartBeatMessage;
        }
    }
}
