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

import java.util.Arrays;

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

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DefaultTransactionReplayState) {
            DefaultTransactionReplayState tState = (DefaultTransactionReplayState) obj;

            if ((this.messageBatches != null && tState.messageBatches == null) ||
                    (this.messageBatches == null && tState.messageBatches != null)) {
                //System.out.println("[DefaultTransactionReplayState] returing FALSE1!");
                return false;
            }

            if (this.messageBatches != null && tState.messageBatches != null) {

                if (this.messageBatches.length != tState.messageBatches.length) {
                    //System.out.println("[DefaultTransactionReplayState] returing FALSE2!");
                    return false;
                }

                for (int i = 0; i < this.messageBatches.length; i++) {

                    if (this.messageBatches[i] == null && tState.messageBatches[i] != null) {
                        //System.out.println("[DefaultTransactionReplayState] returing FALSE3!");
                        return false;
                    }

                    if (this.messageBatches[i] != null && tState.messageBatches[i] == null) {
                        //System.out.println("[DefaultTransactionReplayState] returing FALSE4!");
                        return false;
                    }

                    if (!(this.messageBatches[i] == null && tState.messageBatches[i] == null) &&
                            (!this.messageBatches[i].equals(tState.messageBatches[i]))) {
                        //System.out.println("[DefaultTransactionReplayState] returing FALSE5!" + (this.messageBatches[i] == null) + " " + (tState.messageBatches[i] == null));
                        return false;
                    }
                }
            }
            return (tState.startCid == this.startCid && tState.endCid == this.endCid && tState.pid == this.pid);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = 1;
        hash = hash * 31 + this.startCid;
        hash = hash * 31 + this.endCid;
        hash = hash * 31 + this.pid;
                
        if (this.messageBatches != null) {
            for (int i = 0; i < this.messageBatches.length; i++) {
                if (this.messageBatches[i] != null) {
                    hash = hash * 31 + this.messageBatches[i].hashCode();
                } else {
                    hash = hash * 31 + 0;
                }
            }
        } else {
            hash = hash * 31 + 0;
        }
        return hash;
    }
}
