package bftsmart.tom.leaderchange;

import bftsmart.communication.SystemMessage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Message used during leader change and synchronization
 * @author shaozhuguang
 */
public class LeaderResponseMessage extends SystemMessage {

    private long sequence;

    private int from;

    private int leader;

    public LeaderResponseMessage() {
    }

    /**
     * Constructor
     * @param from replica that creates this message
     * @param leader type of the message (STOP, SYNC, CATCH-UP)
     */
    public LeaderResponseMessage(int from, int leader, long sequence) {
        super(from);
        this.leader = leader;
        this.sequence = sequence;
    }

    public int getFrom() {
        return from;
    }

    public void setFrom(int from) {
        this.from = from;
    }

    public int getLeader() {
        return leader;
    }

    public void setLeader(int leader) {
        this.leader = leader;
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

        out.writeInt(leader);
        out.writeLong(sequence);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);

        leader = in.readInt();

        sequence = in.readLong();
    }
}
