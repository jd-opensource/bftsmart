package bftsmart.tom.core;

import bftsmart.reconfiguration.views.NodeNetwork;
import bftsmart.reconfiguration.views.View;
import bftsmart.tom.core.messages.ViewMessage;
import bftsmart.tom.leaderchange.TimestampMessage;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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

    private static final long SEND_PERIOD = 10000L;

    private final ScheduledExecutorService sendThreadPool = Executors.newSingleThreadScheduledExecutor();

    private final Lock lock = new ReentrantLock();

    private int processId;

    private TOMLayer tomLayer;

    public ViewSyncTimer(TOMLayer tomLayer) {
        this.tomLayer = tomLayer;
        this.processId = tomLayer.controller.getStaticConf().getProcessId();
    }

    public void start() {
        sendThreadPool.scheduleAtFixedRate(() -> {
            lock.lock();
            try {
                View view = tomLayer.controller.getCurrentView();
                ViewMessage viewMessage = new ViewMessage(processId, view);
                tomLayer.getCommunication().send(tomLayer.controller.getCurrentViewOtherAcceptors(), viewMessage);
            } finally {
                lock.unlock();
            }
        }, SEND_PERIOD, SEND_PERIOD, TimeUnit.MILLISECONDS);
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
                        // 是远端节点的配置信息，则更新本地
                        localViewAddresses.put(nodeId, nodeNetwork);
                    } else if (nodeId != processId) {
                        // 非本地节点，则需要进行判断
                        NodeNetwork localNodeNetwork = localViewAddresses.get(nodeId);
                        if (localNodeNetwork == null) {
                            // 若本地不存在该配置，则更新
                            localViewAddresses.put(nodeId, nodeNetwork);
                        } else {
                            // 判断本地配置是否合法
                            if (!checkNodeNetwork(localNodeNetwork)) {
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
