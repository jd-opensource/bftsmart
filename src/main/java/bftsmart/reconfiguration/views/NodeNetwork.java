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
     * 是否开启安全连接
     */
    protected boolean secure;

    public NodeNetwork() {
    }

    public NodeNetwork(String host, int consensusPort, int monitorPort, boolean secure) {
        this.host = host;
        this.consensusPort = consensusPort;
        this.monitorPort = monitorPort;
        this.secure = secure;
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
        return host + ":" + consensusPort + ":" + monitorPort + ":" + secure;
    }

    public boolean isSecure() {
        return secure;
    }

    public void setSecure(boolean secure) {
        this.secure = secure;
    }

    @Override
    public String toString() {
        return "NodeNetwork{" +
                "host='" + host + '\'' +
                ", consensusPort=" + consensusPort +
                ", monitorPort=" + monitorPort +
                ", secure=" + secure +
                '}';
    }
}
