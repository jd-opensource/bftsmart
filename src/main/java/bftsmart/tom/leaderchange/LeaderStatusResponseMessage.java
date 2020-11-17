package bftsmart.tom.leaderchange;


import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Leader状态应答消息
 *
 */
public class LeaderStatusResponseMessage extends LeaderStatusRequestMessage {

    // 正常，表示当前节点认为Leader是正常的
    public static final int LEADER_STATUS_NORMAL = 0;

    // 超时，表示当前节点认为与Leader连接超时（即可以触发LC）
    public static final int LEADER_STATUS_TIMEOUT = 1;

    // 不相同，表示当前节点收到的Leader与发送方不一致
    public static final int LEADER_STATUS_NOTEQUAL = 2;

    private int status;

    public LeaderStatusResponseMessage() {
    }

    public LeaderStatusResponseMessage(int sender, long sequence, int leaderId) {
        super(sender, sequence, leaderId);
    }

    public LeaderStatusResponseMessage(int sender, long sequence, int leaderId, int status) {
        super(sender, sequence, leaderId);
        this.status = status;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(status);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        status = in.readInt();
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }
}
