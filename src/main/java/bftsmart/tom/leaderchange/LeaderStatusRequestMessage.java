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

    public LeaderStatusRequestMessage() {
    }

    public LeaderStatusRequestMessage(int sender, long sequence) {
        super(sender);
        this.sequence = sequence;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeLong(sequence);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        sequence = in.readLong();
    }

    public long getSequence() {
        return sequence;
    }

    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

}
