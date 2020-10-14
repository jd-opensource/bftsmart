package bftsmart.reconfiguration.views;

import java.io.Serializable;

/**
 * 节点网络配置
 *
 */
public class NodeNetwork implements Serializable {

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
        this.monitorPort = monitorPort;
    }

    public String toUrl() {
        return host + ":" + consensusPort + ":" + monitorPort;
    }
}
