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

import bftsmart.statemanagement.TRMessage;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * 落后节点与数据完备源节点进行状态传输，跨checkpoint时发起的交易重放请求消息；
 */

public class TRRequestMessage extends TRMessage {

    /**
     * Constructs a SMMessage
     * @param sender Process Id of the sender
     * @param target Process Id of the target
     * @param startCid Start Consensus ID of Transaction Replay
     * @param endCid End Consensus ID of Transaction Replay
     * @param type Message type
     * @param
     */
    public TRRequestMessage(int sender, int target, int startCid, int endCid, int type) {
        super(sender, target, startCid, endCid, type);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
    }
}
