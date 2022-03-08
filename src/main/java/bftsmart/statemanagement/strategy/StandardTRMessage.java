/**
Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package bftsmart.statemanagement.strategy;

import bftsmart.communication.SystemMessage;
import bftsmart.statemanagement.TransactionReplayState;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * 落后节点与数据完备源节点进行状态传输，跨checkpoint时使用的交换消息类型
 * 
 *
 */
public class StandardTRMessage extends SystemMessage {

    private int target;
    private int startCid;
    private int endCid;
    private int type;
    private TransactionReplayState state;

    /**
     * Constructs a SMMessage
     * @param sender Process Id of the sender
     * @param target Process Id of the target
     * @param startCid Start Consensus ID of Transaction Replay
     * @param endCid End Consensus ID of Transaction Replay
     * @param type Message type
     * @param
     */
    public StandardTRMessage(int sender, int target, TransactionReplayState state, int startCid, int endCid, int type) {
        super(sender);
        this.target = target;
        this.startCid = startCid;
        this.endCid = endCid;
        this.type = type;
        this.state = state;
    }

    public StandardTRMessage() {
        super();
    }

    public int getTarget() {
        return target;
    }

    public int getStartCid() {
        return startCid;
    }

    public int getEndCid() {
        return endCid;
    }

    /**
     * Retrieves the type of the message
     * @return The type of the message
     */
    public int getType() {
        return type;
    }

    public TransactionReplayState getState() {
        return state;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeInt(target);
        out.writeInt(startCid);
        out.writeInt(endCid);
        out.writeInt(type);
        out.writeObject(state);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        target = in.readInt();
        startCid = in.readInt();
        endCid = in.readInt();
        type = in.readInt();
        state = (TransactionReplayState) in.readObject();
    }

    @Override
    public String toString() {
        return "StandardTRMessage{" +
                "sender=" + getSender() + "," +
                "startCid=" + getStartCid() + "," +
                "type=" + getType() + "," +
                "endCid=" + getEndCid() +
                '}';
    }
}
