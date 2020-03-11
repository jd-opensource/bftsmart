package bftsmart.tom.leaderchange;

import bftsmart.communication.ServerCommunicationSystem;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.tom.core.TOMLayer;
import org.apache.commons.collections4.map.LRUMap;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This thread serves as a manager for all timers of pending requests.
 *
 */
public class HeartBeatTimer {

    // 重复发送LeaderRequest的间隔时间
    private static final long RESEND_MILL_SECONDS = 5000;

    private final Map<Long, List<LeaderResponseMessage>> leaderResponseMap = new LRUMap<>(1024 * 8);

    private Timer leaderTimer = new Timer("heart beat leader timer");

    private Timer replicaTimer = new Timer("heart beat replica timer");

    private Timer leaderResponseTimer = new Timer("get leader response timer");

    private RequestsTimer requestsTimer;

    private TOMLayer tomLayer; // TOM layer

    private volatile InnerHeartBeatMessage innerHeartBeatMessage;

    private volatile long lastLeaderRequestSequence = -1L;

    private Lock lrLock = new ReentrantLock();

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
            } else {
                sendLeaderRequestMessage();
            }
        } finally {
            hbLock.unlock();
        }
    }

    /**
     * 收到领导者请求
     * @param requestMessage
     */
    public void receiveLeaderRequestMessage(LeaderRequestMessage requestMessage) {
        // 获取当前节点的领导者信息，然后应答给发送者
        int currLeader = tomLayer.leader();
        LeaderResponseMessage responseMessage = new LeaderResponseMessage(
                controller.getStaticConf().getProcessId(), currLeader, requestMessage.getSequence());
        int[] to = new int[1];
        to[0] = requestMessage.getFrom();
        communication.send(to, responseMessage);
    }

    /**
     * 收到领导者应答请求
     * @param responseMessage
     */
    public void receiveLeaderResponseMessage(LeaderResponseMessage responseMessage) {
        // 判断是否是自己发送的sequence
        lrLock.lock();
        try {
            long msgSeq = responseMessage.getSequence();
            if (msgSeq == lastLeaderRequestSequence) {
                // 是当前节点发送的请求，则将其加入到Map中
                List<LeaderResponseMessage> responseMessages = leaderResponseMap.get(msgSeq);
                if (responseMessages == null) {
                    responseMessages = new ArrayList<>();
                    responseMessages.add(responseMessage);
                    leaderResponseMap.put(msgSeq, responseMessages);
                } else {
                    responseMessages.add(responseMessage);
                }
                // 判断收到的心跳信息是否满足
                int newLeaderId = newLeader(responseMessage.getSequence());
                if (newLeaderId >= 0) {
                    // 表示满足条件，设置新的Leader
                    leaderResponseTimer.cancel(); // 取消定时器
                    leaderResponseTimer = null;
                    //如果我本身是领导者，又收到来自其他领导者的心跳，经过领导者查询之后需要取消一个领导者定时器
                    if (tomLayer.leader() != newLeaderId) {
                        if (leaderTimer != null) {
                            leaderTimer.cancel();
                            leaderTimer = null;
                        } else if (replicaTimer != null) {
                           //To be perfected
                        }
                    }
                    tomLayer.execManager.setNewLeader(newLeaderId); // 设置新的Leader
                }
            } else {
                // 收到的心跳信息有问题，打印日志
                System.out.printf("receive leader response last sequence = %s, receive sequence = %s \r\n",
                        lastLeaderRequestSequence, msgSeq);
            }
        } finally {
            lrLock.unlock();
        }
    }

    public void setCommunication(ServerCommunicationSystem communication) {
        this.communication = communication;
    }

    public void sendLeaderRequestMessage() {
        // 假设收到的消息不是当前的Leader，则需要发送获取其他节点Leader
        long sequence = System.currentTimeMillis();
        lrLock.lock();
        try {
            // 防止一段时间内重复发送多次心跳请求
            if (sequence - lastLeaderRequestSequence > RESEND_MILL_SECONDS) {
                sendLeaderRequestMessage(sequence);
            }
        } finally {
            lrLock.unlock();
        }
    }

    private void sendLeaderRequestMessage(long sequence) {
        LeaderRequestMessage requestMessage = new LeaderRequestMessage(
                controller.getStaticConf().getProcessId(), sequence);
        communication.send(controller.getCurrentViewOtherAcceptors(), requestMessage);
        lastLeaderRequestSequence = sequence;
        // 启动定时任务，判断心跳的应答处理
        if (leaderResponseTimer == null) {
            leaderResponseTimer = new Timer("get leader response timer");
        }
        leaderResponseTimer.schedule(new LeaderResponseTask(lastLeaderRequestSequence), RESEND_MILL_SECONDS);
    }

    /**
     * 获取新的Leader
     * @return
     *     返回-1表示未达成一致，否则表示达成一致
     */
    private int newLeader(long currentSequence) {
        // 从缓存中获取应答
        List<LeaderResponseMessage> leaderResponseMessages = leaderResponseMap.get(currentSequence);
        if (leaderResponseMessages == null || leaderResponseMessages.isEmpty()) {
            return -1;
        } else {
            // 判断收到的应答结果是不是满足2f+1的规则
            Map<Integer, Integer> leader2Size = new HashMap<>();
            // 防止重复
            Set<Integer> nodeSet = new HashSet<>();
            for (LeaderResponseMessage lrm : leaderResponseMessages) {
                int currentLeader = lrm.getLeader();
                int currentNode = lrm.getFrom();
                if (!nodeSet.contains(currentNode)) {
                    leader2Size.merge(currentLeader, 1, Integer::sum);
                    nodeSet.add(currentNode);
                }
            }
            // 获取leaderSize最大的Leader
            int leaderMaxSize = -1;
            int leaderMaxId = -1;
            for (Map.Entry<Integer, Integer> entry : leader2Size.entrySet()) {
                int currLeaderId = entry.getKey(), currLeaderSize = entry.getValue();
                if (currLeaderSize > leaderMaxSize) {
                    leaderMaxId = currLeaderId;
                    leaderMaxSize = currLeaderSize;
                }
            }
            // 判断是否满足2f+1
            int compareSize = 2 * controller.getStaticConf().getF() + 1;
            if (leaderMaxSize >= compareSize) {
                return leaderMaxId;
            }
            return -1;
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

    class LeaderResponseTask extends TimerTask {

        private final long currentSequence;

        LeaderResponseTask(long currentSequence) {
            this.currentSequence = currentSequence;
        }

        @Override
        public void run() {
            lrLock.lock();
            try {
                int newLeaderId = newLeader(currentSequence);
                if (newLeaderId < 0) {
                    // 不满足，则重新发送获取
                    leaderResponseTimer = null;
                    sendLeaderRequestMessage(System.currentTimeMillis());
                } else {
                    // 满足，则更新当前节点的Leader
                    tomLayer.execManager.setNewLeader(newLeaderId);
                }
            } finally {
                lrLock.unlock();
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
