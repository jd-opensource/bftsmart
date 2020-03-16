package bftsmart.tom.leaderchange;

import bftsmart.communication.SystemMessage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Message used during leader change and synchronization
 * @author shaozhuguang
 */
public class LeaderRequestMessage extends SystemMessage {

    private long sequence;

    public LeaderRequestMessage() {
    }

    /**
     * Constructor
     * @param from replica that creates this message
     */
    public LeaderRequestMessage(int from, long sequence) {
        super(from);
        this.sequence = sequence;
    }

    public long getSequence() {
        return sequence;
    }

    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeLong(sequence);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        sequence = in.readLong();
    }
}
