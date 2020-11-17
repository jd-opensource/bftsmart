package bftsmart.tom.leaderchange;

import bftsmart.communication.SystemMessage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Leader状态请求
 *
 */
public class LeaderStatusRequestMessage extends SystemMessage {

    // 序号，用于处理应答的Key
    private long sequence;

    // 当前节点的LeaderID
    private int leaderId;

    public LeaderStatusRequestMessage() {
    }

    public LeaderStatusRequestMessage(int sender, long sequence, int leaderId) {
        super(sender);
        this.sequence = sequence;
        this.leaderId = leaderId;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeLong(sequence);
        out.writeInt(leaderId);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        sequence = in.readLong();
        leaderId = in.readInt();
    }

    public long getSequence() {
        return sequence;
    }

    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
    }
}
