package bftsmart.reconfiguration.views;

/**
 * 节点网络配置
 */
public class NullNodeNetwork extends NodeNetwork {

    public NullNodeNetwork() {
        this.host = null;
        this.consensusPort = -1;
        this.monitorPort = -1;
        this.secure = false;
    }
}
