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
package bftsmart.statemanagement;

import bftsmart.reconfiguration.ViewTopology;
import bftsmart.tom.leaderchange.CertifiedDecision;

import java.io.Serializable;

/**
 * 数据源完备节点向落后节点提供需要进行重放的交易状态接口
 * @param getStartCid 重放交易起始cid
 * @param getEndCid 重放交易结束cid
 * @author
 */
public interface TransactionReplayState extends Serializable {

    public int getStartCid();

    public int getEndCid();

    public int getPid();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract int hashCode();

}
