package bftsmart.tom.leaderchange;

import bftsmart.communication.SystemMessage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class TimestampMessage extends SystemMessage {

    private long timestamp;

    public TimestampMessage() {
    }

    /**
     * Constructor
     * @param from replica that creates this message
     * @param timestamp
     */
    public TimestampMessage(int from, long timestamp) {
        super(from);
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeLong(timestamp);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        timestamp = in.readLong();
    }
}
