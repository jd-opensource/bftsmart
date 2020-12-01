package bftsmart.reconfiguration.views;

import bftsmart.tom.core.ViewSyncTimer;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * 节点网络配置
 *
 */
public class NodeNetwork implements Serializable {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(NodeNetwork.class);

    /**
     * 域名
     *
     */
    private String host;

    /**
     * 共识端口
     *
     */
    private int consensusPort;

    /**
     * 管理端口
     *
     */
    private int monitorPort;

    public NodeNetwork() {
    }

    public NodeNetwork(String host, int consensusPort, int monitorPort) {
        this.host = host;
        this.consensusPort = consensusPort;
        this.monitorPort = monitorPort;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getConsensusPort() {
        return consensusPort;
    }

    public void setConsensusPort(int consensusPort) {
        this.consensusPort = consensusPort;
    }

    public int getMonitorPort() {
        return monitorPort;
    }

    public void setMonitorPort(int monitorPort) {
        LOGGER.info("host[{}] setMonitorPort-> {} !", host, monitorPort);
        this.monitorPort = monitorPort;
    }

    public String toUrl() {
        return host + ":" + consensusPort + ":" + monitorPort;
    }

    @Override
    public String toString() {
        return "[" +
                "host='" + host + '\'' +
                ", consensusPort=" + consensusPort +
                ", monitorPort=" + monitorPort +
                ']';
    }
}
