package bftsmart.consensus.messages;

import bftsmart.communication.SystemMessage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Heart Beat Message
 *
 * @author shaozhuguang
 */
public class HeartBeatMessage extends SystemMessage {

    private int leader;

    public HeartBeatMessage() {
    }

    /**
     * Constructor
     * @param from replica that creates this message
     * @param leader type of the message (STOP, SYNC, CATCH-UP)
     */
    public HeartBeatMessage(int from, int leader) {
        super(from);
        this.leader = leader;
    }

    public int getLeader() {
        return leader;
    }

    public void setLeader(int leader) {
        this.leader = leader;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeInt(leader);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        leader = in.readInt();
    }
}
