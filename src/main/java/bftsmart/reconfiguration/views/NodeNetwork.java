package bftsmart.reconfiguration.views;

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
    protected String host;

    /**
     * 共识端口
     *
     */
    protected int consensusPort;

    /**
     * 管理端口
     *
     */
    protected int monitorPort;

    /**
     * 共识服务是否开启安全连接
     */
    protected boolean consensusSecure;

    /**
     * 管理服务是否开启安全连接
     */
    protected boolean monitorSecure;

    public NodeNetwork() {
    }

    public NodeNetwork(String host, int consensusPort, int monitorPort, boolean consensusSecure, boolean monitorSecure) {
        this.host = host;
        this.consensusPort = consensusPort;
        this.monitorPort = monitorPort;
        this.consensusSecure = consensusSecure;
        this.monitorSecure = monitorSecure;
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

    public boolean isMonitorSecure() {
        return monitorSecure;
    }

    public void setMonitorSecure(boolean monitorSecure) {
        this.monitorSecure = monitorSecure;
    }

    public boolean isConsensusSecure() {
        return consensusSecure;
    }

    public void setConsensusSecure(boolean consensusSecure) {
        this.consensusSecure = consensusSecure;
    }

    @Override
    public String toString() {
        return "NodeNetwork{" +
                "host='" + host + '\'' +
                ", consensusPort=" + consensusPort +
                ", monitorPort=" + monitorPort +
                ", consensusSecure=" + consensusSecure +
                ", monitorSecure=" + monitorSecure +
                '}';
    }
}
