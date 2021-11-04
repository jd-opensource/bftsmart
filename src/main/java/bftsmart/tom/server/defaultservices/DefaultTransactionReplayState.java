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
package bftsmart.tom.server.defaultservices;

import bftsmart.statemanagement.TransactionReplayState;
import org.slf4j.LoggerFactory;

/**
 *数据源完备节点向落后节点提供需要进行重放的交易状态类
 * @param messageBatches 交易重放状态内容
 * @param startCid 交易重放起始共识ID
 * @param endCid 交易重放结束共识ID
 * @param pid 提供交易重放的处理器ID
 * @author
 */
public class DefaultTransactionReplayState implements TransactionReplayState {

    private static final long serialVersionUID = 6771081456095596311L;

    protected int startCid = -1; // Consensus ID for the last messages batch delivered to the application

    protected int endCid = -1;

    private CommandsInfo[] messageBatches; // batches received since the last checkpoint.

    private int pid;

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(DefaultTransactionReplayState.class);


    public DefaultTransactionReplayState(CommandsInfo[] messageBatches, int startCid, int endCid, int pid) {

        this.messageBatches = messageBatches;
        this.startCid = startCid;
        this.endCid = endCid;
        this.pid = pid;
    }

    @Override
    public int getStartCid() {
        return startCid;
    }


    @Override
    public int getEndCid() {
        return endCid;
    }

    @Override
    public int getPid() {
        return pid;
    }

    public CommandsInfo[] getMessageBatches() {
        return messageBatches;
    }

    public void setMessageBatches(CommandsInfo[] messageBatches) {
    	this.messageBatches = messageBatches;
    }

}
