package bftsmart.tom.core;

import bftsmart.reconfiguration.views.NodeNetwork;
import bftsmart.reconfiguration.views.View;
import bftsmart.tom.core.messages.ViewMessage;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 视图同步定时器
 *
 */
public class ViewSyncTimer {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ViewSyncTimer.class);

    /**
     * 视图同步周期
     *         单位：毫秒
     */
    private static final long SEND_PERIOD = 10000L;

    private static final long SEND_DELAY = 5000L;

    private final ScheduledExecutorService sendThreadPool = Executors.newSingleThreadScheduledExecutor();

    private final Lock lock = new ReentrantLock();

    private int processId;

    private TOMLayer tomLayer;

    public ViewSyncTimer(TOMLayer tomLayer) {
        this.tomLayer = tomLayer;
        this.processId = tomLayer.controller.getStaticConf().getProcessId();
    }

    public void start() {
        sendThreadPool.scheduleWithFixedDelay(() -> {
            lock.lock();
            try {
                View view = tomLayer.controller.getCurrentView();
                ViewMessage viewMessage = new ViewMessage(processId, view);
                int[] otherAcceptors = tomLayer.controller.getCurrentViewOtherAcceptors();
                for (int id : otherAcceptors) {
                    LOGGER.info("I am {}, send view message to {} !", processId, id);
                }
                tomLayer.getCommunication().send(tomLayer.controller.getCurrentViewOtherAcceptors(), viewMessage);
            } finally {
                lock.unlock();
            }
        }, SEND_DELAY, SEND_PERIOD, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        sendThreadPool.shutdownNow();
    }

    /**
     * 根据远端的信息更新本地视图
     *
     * @param remoteId
     * @param remoteView
     */
    public void updateView(final int remoteId, final View remoteView) {
        /**
         * 处理逻辑：
         * 1、根据remoteId对应的地址，更新其对应网络配置；
         * 2、若本地视图中除自身及remoteId外的网络配置为空，而view中不为空则使用其结果
         */
        lock.lock();
        try {
            View localView = tomLayer.controller.getCurrentView();
            Map<Integer, NodeNetwork> localViewAddresses = localView.getAddresses();
            // 获取远端的地址列表
            Map<Integer, NodeNetwork> remoteViewAddresses = remoteView.getAddresses();
            for (Map.Entry<Integer, NodeNetwork> entry : remoteViewAddresses.entrySet()) {
                int nodeId = entry.getKey();
                NodeNetwork nodeNetwork = entry.getValue();
                if (checkNodeNetwork(nodeNetwork)) {
                    if (nodeId == remoteId) {
                        LOGGER.info("Receive remote[{}]'s view message, node[{}]'s network = [{}] !", remoteId, nodeId, nodeNetwork.toUrl());
                        // 是远端节点的配置信息，则更新本地
                        localViewAddresses.put(nodeId, nodeNetwork);
                    } else if (nodeId != processId) {
                        // 非本地节点，则需要进行判断
                        NodeNetwork localNodeNetwork = localViewAddresses.get(nodeId);
                        if (localNodeNetwork == null) {
                            LOGGER.info("Receive remote[{}]'s view message, update node[{}]'s network = [{}] because local is NULL !", remoteId, nodeId, nodeNetwork.toUrl());
                            // 若本地不存在该配置，则更新
                            localViewAddresses.put(nodeId, nodeNetwork);
                        } else {
                            // 判断本地配置是否合法
                            if (!checkNodeNetwork(localNodeNetwork)) {
                                LOGGER.info("Receive remote[{}]'s view message, update node[{}]'s network = [{}] because local is illegal !", remoteId, nodeId, nodeNetwork.toUrl());
                                // 本地不合法，表示本地配置信息不合法，可以更新
                                localViewAddresses.put(nodeId, nodeNetwork);
                            }
                        }
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 判断地址是否合法
     *
     * @param nodeNetwork
     * @return
     */
    private boolean checkNodeNetwork(NodeNetwork nodeNetwork) {
        if (nodeNetwork != null) {
            String host = nodeNetwork.getHost();
            return host != null && host.length() > 0 &&
                    nodeNetwork.getMonitorPort() > 0 &&
                    nodeNetwork.getConsensusPort() > 0;
        }
        return false;
    }
}
