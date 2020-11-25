package bftsmart.reconfiguration.views;

import java.io.Serializable;

/**
 * 节点网络配置
 *
 */
public class NullNodeNetwork extends NodeNetwork {

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

    public NullNodeNetwork() {
        this.host = null;
        this.consensusPort = -1;
        this.monitorPort = -1;
    }

    @Override
    public String getHost() {
        return null;
    }


    @Override
    public int getConsensusPort() {
        return -1;
    }

    @Override
    public int getMonitorPort() {
        return -1;
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
