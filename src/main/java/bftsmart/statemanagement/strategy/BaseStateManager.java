/**
 * Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and
 * the authors indicated in the @author tags
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package bftsmart.statemanagement.strategy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantLock;

import bftsmart.statemanagement.TransactionReplayState;
import bftsmart.tom.MessageContext;
import bftsmart.tom.server.defaultservices.CommandsInfo;
import bftsmart.tom.server.defaultservices.DefaultRecoverable;
import bftsmart.tom.server.defaultservices.DefaultTransactionReplayState;
import org.slf4j.LoggerFactory;

import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.reconfiguration.ReplicaTopology;
import bftsmart.reconfiguration.views.View;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.SMMessage;
import bftsmart.statemanagement.StateManager;
import bftsmart.tom.core.DeliveryThread;
import bftsmart.tom.core.TOMLayer;
import bftsmart.tom.leaderchange.CertifiedDecision;
import bftsmart.tom.leaderchange.LCManager;
import bftsmart.tom.util.TOMUtil;

/**
 *
 * @author Marcel Santos
 *
 */
public abstract class BaseStateManager implements StateManager {

    protected TOMLayer tomLayer;
    protected ReplicaTopology topology;
    protected DeliveryThread dt;

    protected HashMap<Integer, ApplicationState> senderStates = null;
    protected HashMap<Integer, View> senderViews = null;
    protected HashMap<Integer, Integer> senderRegencies = null;
    protected HashMap<Integer, Integer> senderLeaders = null;
    protected HashMap<Integer, CertifiedDecision> senderProofs = null;

    protected boolean appStateOnly;
    protected volatile int waitingCID = -1;
    protected int lastCID;
    protected ApplicationState state;

    protected volatile boolean isInitializing = true;

    protected volatile boolean doWork = true;

    protected int STATETRANSFER_RETRY_COUNT = 500;

//    private int checkPointFormOtherNode = -1;

    private HashMap<Integer, Integer> senderCIDs = null;

    private HashMap<Integer, Integer> senderVIDs = null;

    private HashMap<Integer, Integer> validDataSenders = null;

    private HashMap<Integer, TransactionReplayState> replayStateHashMap = null;

    private ReentrantLock replayReceivedLock = new ReentrantLock();

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(BaseStateManager.class);

    public BaseStateManager() {
        senderStates = new HashMap<>();
        senderViews = new HashMap<>();
        senderRegencies = new HashMap<>();
        senderLeaders = new HashMap<>();
        senderProofs = new HashMap<>();
        validDataSenders = new HashMap<>();
        replayStateHashMap = new HashMap<>();
    }

    public HashMap<Integer, TransactionReplayState> getReplayStateHashMap() {
        return replayStateHashMap;
    }

    protected int getReplies() {
        return senderStates.size();
    }

    protected boolean enoughReplies() {
//        return senderStates.size() > SVController.getCurrentViewF();
        return senderStates.size() >= topology.getQuorum();
    }

    protected boolean enoughRegencies(int regency) {
        return senderRegencies.size() >= topology.getQuorum();
    }

    protected boolean enoughLeaders(int leader) {
        return senderLeaders.size() >= topology.getQuorum();
    }

    protected boolean enoughViews(View view) {
        Collection<View> views = senderViews.values();
        int counter = 0;
        for (View v : views) {
            if (view.equals(v)) {
                counter++;
            }
        }
        boolean result = counter >= topology.getQuorum();
        return result;
    }

    protected boolean enoughState(ApplicationState state) {
        Collection<ApplicationState> states = senderStates.values();
        int counter = 0;
        for (ApplicationState s: states) {
            if (state.equals(s)) {
                counter++;
            }
        }
        boolean result = counter >= topology.getQuorum();
        return result;
    }


    // check if the consensus messages are consistent without checking the mac/signatures
    // if it is consistent, it returns the respective consensus ID; otherwise, returns -1
    private int proofIsConsistent(Set<ConsensusMessage> proof) {
        
        int id = -1;
        byte[] value = null;
        
        for (ConsensusMessage cm : proof) {
            
            if (id == -1) id = cm.getNumber();
            if (value == null) value = cm.getValue();
            
            if (id != cm.getNumber() || !Arrays.equals(value, cm.getValue())) {
                return -1; // they are not consistent, so the proof is invalid
            }
                    
        }
        
        // if the values are still these, this means the proof is empty, thus is invalid
        if (id == -1 || value == null) return -1;
        
        return id;
    }
        
    protected boolean enoughProofs(int cid, LCManager lc) {
        
        int counter = 0;
        int nullCounter = 0;

        for (CertifiedDecision cDec : senderProofs.values()) {
                                    
            if (cDec != null && cid == proofIsConsistent(cDec.getConsMessages()) && lc.hasValidProof(cDec)) {
                counter++;
            } else if (cDec == null) {
                nullCounter++;
            }
            
        }
        boolean result = (counter >= topology.getQuorum()) || (nullCounter >= topology.getQuorum());
        return result;
    }
    
    /**
     * Clear the collections and state hold by this object. Calls clear() in the
     * States, Leaders, regencies and Views collections. Sets the state to
     * null;
     */
    protected void reset() {
        senderStates.clear();
        senderLeaders.clear();
        senderRegencies.clear();
        senderViews.clear();
        senderProofs.clear();
        replayStateHashMap.clear();
        state = null;
    }

    public Collection<ApplicationState> receivedStates() {
        return senderStates.values();
    }

    @Override
    public void setLastCID(int cid) {
        lastCID = cid;
    }

    @Override
    public int getLastCID() {
        return lastCID;
    }

    @Override
    public void requestAppState(int cid) {
        lastCID = cid + 1;
        waitingCID = cid;
        LOGGER.info("waitingcid is now {}", cid);
        appStateOnly = true;
        requestState();
    }

    @Override
    public void analyzeState(int cid) {
       LOGGER.info("BaseStateManager.running.analyzeState: The state transfer protocol is enabled");
        if (waitingCID == -1) {
           LOGGER.info("BaseStateManager.running.analyzeState: I'm not waiting for any state, so I will keep record of this message");
            if (tomLayer.execManager.isDecidable(cid)) {
                LOGGER.info("BaseStateManager.running.analyzeState: I have now more than {} messages for CID {} which are beyond CID {}", topology.getCurrentViewF(), cid, lastCID);

                waitingCID = cid - 1; // 设置为cid - 1, 是为了避免数据源节点lastcid 还没有更新完成
                LOGGER.info("analyzeState {}", waitingCID);

                int checkPointFromOtherNode = this.tomLayer.controller.getStaticConf().getCheckpointPeriod() * (cid / this.tomLayer.controller.getStaticConf().getCheckpointPeriod()) - 1;

                if (lastCID < checkPointFromOtherNode) {
                    LOGGER.info("BaseStateManager.running.analyzeState: Backward node cross checkpoint, will start transactions replay process!");
                    // 启动交易重放过程,随机选择data sender
                    askTransactionReplay(lastCID + 1, checkPointFromOtherNode);
                } else {
                    // 通用流程
                    LOGGER.info("BaseStateManager.running.analyzeState: Backward node and remote node in the same latest checkpoint!");
                    // 请求最新checkpoint周期内的交易并重放
                    requestState();
                }
            }
        }
    }

    @Override
    public abstract void init(TOMLayer tomLayer, DeliveryThread dt);

    @Override
    public boolean isRetrievingState() {
        if (isInitializing) {
            return true;
        }
        return waitingCID > -1;
    }

    @Override
    public void askCurrentConsensusId() {
        int counter = 0;
        int me = topology.getStaticConf().getProcessId();
        int[] target = topology.getCurrentViewProcesses();

        SMMessage currentCID = new StandardSMMessage(me, -1, TOMUtil.SM_ASK_INITIAL, 0, null, this.topology.getCurrentView(), 0, 0);
        LOGGER.info("I will send StandardSMMessage[{}] to all nodes !", TOMUtil.SM_ASK_INITIAL);
        tomLayer.getCommunication().send(target, currentCID);

        target = topology.getCurrentViewOtherAcceptors();

        while (isInitializing && doWork) {
            LOGGER.info("I am proc {}, I will send StandardSMMessage[{}] to others !", topology.getStaticConf().getProcessId(), TOMUtil.SM_ASK_INITIAL);
            tomLayer.getCommunication().send(target, currentCID);
            try {
                Thread.sleep(1500);
                if (++counter > STATETRANSFER_RETRY_COUNT) {
                    LOGGER.info("###############################################################################################################################################################");
                    LOGGER.info("################State Transfer No Replier, Maybe Requester View Is Obsolete, If Block Exist Diff, Please Copy Ledger Database And Restart!#####################");
                    LOGGER.info("###############################################################################################################################################################");
                    break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void currentConsensusIdAsked(int sender, int viewId) {
        LOGGER.info("I will handle currentConsensusIdAsked, sender = {} !", sender);
        if (viewId < this.topology.getCurrentView().getId()) {
            LOGGER.info("#######################################################################################################################################################");
            LOGGER.info("################State Transfer Requester View Is Obsolete, If Block Exist Diff, Please Requester Copy Ledger Database And Restart!#####################");
            LOGGER.info("#######################################################################################################################################################");
        }
        int me = topology.getStaticConf().getProcessId();
        int lastConsensusId = tomLayer.getLastExec();
        SMMessage currentCIDReply = new StandardSMMessage(me, lastConsensusId, TOMUtil.SM_REPLY_INITIAL, 0, null, this.topology.getCurrentView(), 0, 0);
        tomLayer.getCommunication().send(new int[]{sender}, currentCIDReply);
    }

    @Override
    public synchronized void currentConsensusIdReceived(SMMessage smsg) {
        LOGGER.info("I am proc {}, I will handle currentConsensusIdReceived!", topology.getStaticConf().getProcessId());

        try {
            if (!isInitializing || waitingCID > -1) {
                LOGGER.info("isInitializing = {}, and waitingCID= {} !", isInitializing, waitingCID);
                return;
            }

            if (senderCIDs == null) {
                senderCIDs = new HashMap<>();
            }
            senderCIDs.put(smsg.getSender(), smsg.getCID());

            if (senderVIDs == null) {
                senderVIDs = new HashMap<>();
            }
            senderVIDs.put(smsg.getSender(), smsg.getView().getId());

            // Report if view is inconsistent
            checkViewInfo(this.topology.getCurrentView().getId(), senderVIDs);

            LOGGER.info("I am proc {}, currentConsensusIdReceived ->sender[{}], CID = {} !", topology.getStaticConf().getProcessId(), smsg.getSender(), smsg.getCID());
            LOGGER.info("I am proc {}, senderCIDs.size() = {}, and SVController.getQuorum()= {} !", topology.getStaticConf().getProcessId(), senderCIDs.size(), topology.getQuorum());
            if (senderCIDs.size() >= topology.getQuorum()) {

                HashMap<Integer, Integer> cids = new HashMap<>();
                for (int id : senderCIDs.keySet()) {

                    int value = senderCIDs.get(id);

                    Integer count = cids.get(value);
                    if (count == null) {
                        cids.put(value, 1);
                    } else {
                        cids.put(value, count + 1);
                    }
                }
                for (int key : cids.keySet()) {
                    LOGGER.info("key = {}, cids.get(key) = {}, SVController.getQuorum() = {} !", key, cids.get(key), topology.getQuorum());
                    if (cids.get(key) >= topology.getQuorum()) {

                        // 保存数据完备节点信息
                        saveValidDataSenders(senderCIDs, key);

                        // 根据数据完备节点的key，计算其检查点值
                        int checkPointFromOtherNode = this.tomLayer.controller.getStaticConf().getCheckpointPeriod() * (key / this.tomLayer.controller.getStaticConf().getCheckpointPeriod()) - 1;

                        if (lastCID >= key) {
                            LOGGER.info("-- {}  replica state is up to date ! --", topology.getStaticConf().getProcessId());
                            dt.deliverLock();
                            isInitializing = false;
                            tomLayer.setLastExec(key);
                            // 如果有其他节点没有差异的话，会走此分支，此时将其连接设置为OK
                            tomLayer.connectRemotesOK();
                            // trigger out of context propose msg process
                            tomLayer.processOutOfContext();
                            dt.canDeliver();
                            dt.deliverUnlock();
                            break;
                        }  else {
                            waitingCID = key;
                            LOGGER.info("-- Requesting state from other replicas, remote node cid = {}, local node lastCid = {}", key, lastCID);
                            // 对于跨checkpoint的落后节点，先通过与数据完备节点的消息交互进行checkpoint以内交易的重放，再执行最新checkpoint周期内交易的重放
                            if (lastCID < checkPointFromOtherNode) {
                                LOGGER.info("-- Backward node cross checkpoint, will start transactions replay process!");
                                // 启动交易重放过程,随机选择目标节点
                                getReplayStateHashMap().clear();
                                askTransactionReplay(lastCID + 1, checkPointFromOtherNode);
                            } else {
                                // 通用流程
                                LOGGER.info("-- Backward node and remote node in the same latest checkpoint!");
                                // 请求最新checkpoint周期内的交易并重放
                                requestState();
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.info("currentConsensusIdReceived occur exception!");
        }

    }

    @Override
    public void transactionReplayReplyDeliver(StandardTRMessage msg) {
        replayReceivedLock.lock();

        int checkPointFromOtherNode = this.tomLayer.controller.getStaticConf().getCheckpointPeriod() * (waitingCID / this.tomLayer.controller.getStaticConf().getCheckpointPeriod()) - 1;

        LOGGER.info("I am proc {}, I will handle transactionReplayReceived!", tomLayer.getCurrentProcessId());
        try {
            replayStateHashMap.put(msg.getStartCid(), ((StandardTRMessage) msg).getState());

            int lastCid = this.tomLayer.getStateManager().getLastCID();
            // 保证交易内容按顺序进行重放
            while (replayStateHashMap.keySet().contains(lastCid + 1)) {

                TransactionReplayState replayState = replayStateHashMap.get(lastCid + 1);

                for (int i = 0, cid = replayState.getStartCid(); (i <= replayState.getEndCid() - replayState.getStartCid()) && (cid <= replayState.getEndCid()); i++,cid++ ) {
                    byte[][] commands = ((DefaultTransactionReplayState) replayState).getMessageBatches()[i].commands;
                    MessageContext[] messageContexts =  ((DefaultTransactionReplayState) replayState).getMessageBatches()[i].msgCtx;
                    LOGGER.info("I am proc {}, I will execute transactions replay!,replay cid = {}", tomLayer.getCurrentProcessId(), cid );
                    ((DefaultRecoverable) tomLayer.getDeliveryThread().getRecoverer()).appExecuteBatch(commands, messageContexts, false);
                }
                this.tomLayer.getStateManager().setLastCID(replayState.getEndCid());
                this.tomLayer.setLastExec(replayState.getEndCid());
                lastCid = replayState.getEndCid();
            }

            if (lastCID == checkPointFromOtherNode) {
                if (replayTimer != null) replayTimer.cancel();
                getReplayStateHashMap().clear();
                requestState();
            }
        } finally {
            replayReceivedLock.unlock();
        }
    }

    protected Timer replayTimer = null;
    protected final static long INIT_TIMEOUT = 20000;
    protected long timeout = INIT_TIMEOUT;

    @Override
    public void askTransactionReplay(int startCid, int endCid) {

        int target = (Integer) validDataSenders.keySet().toArray()[0];

        int me = topology.getCurrentProcessId();
        StandardTRMessage trRequestMessage = new StandardTRMessage(me, target, null, startCid, endCid, TOMUtil.SM_TRANSACTION_REPLAY_REQUEST_INFO);
        LOGGER.info("I will send StandardTRMessage[{}] to target node {}, between cid {} --> {} !", TOMUtil.SM_TRANSACTION_REPLAY_REQUEST_INFO, target, startCid, endCid);
        tomLayer.getCommunication().send(trRequestMessage, target);

        // 交易重放消息的安全保障
        TimerTask replayTask =  new TimerTask() {
            public void run() {
                if (!doWork) {
                    return;
                }
                LOGGER.info("Change valid peer node, Resend timeout transaction replay");
                int newTarget= changeValidDataSender();
                tomLayer.getCommunication().send(trRequestMessage, newTarget);
            }
        };

        replayTimer = new Timer("tx replay timer");
        timeout = timeout * 2;
        replayTimer.schedule(replayTask,timeout);
    }

    private int changeValidDataSender() {

        Object[] processes = validDataSenders.keySet().toArray();

        int target = -1;

        Random r = new Random();

        int pos;

        if (processes != null && processes.length != 0) {
            pos = r.nextInt(processes.length);
            target = (Integer) processes[pos];
        }

        return target;
    }

    @Override
    public void transactionReplayAsked(int sender, int target, int startCid, int endCid) {

        LOGGER.info("I am proc {}, I will handle transactionReplayAsked from = {}!", tomLayer.getCurrentProcessId(), sender);

        StandardTRMessage trReplyMessage;

        // 用来保证本地共识ID，以及tom config 文件配置完成
        if (!tomLayer.isLastCidSetOk()) {
            LOGGER.info("I am proc {}, ignore request cid msg, wait tomlayer set last cid!", tomLayer.getCurrentProcessId());
            return;
        }

        if (target != topology.getCurrentProcessId()) {
            LOGGER.info("I am proc {}, I am not transactions replay reply target!", tomLayer.getCurrentProcessId());
            return;
        }
        int batchSize = 1000;
        for (int cid = startCid; cid <= endCid;) {
            // 交易重放批大小超过batchSize，则以batchSize为单位打包响应消息，否则根据实际大小打包
            if (cid + batchSize -1 < endCid ) {
                TransactionReplayState state = getReplayState(cid,  cid + batchSize - 1);
                trReplyMessage = new StandardTRMessage(topology.getCurrentProcessId(), sender, state, cid, cid + batchSize - 1, TOMUtil.SM_TRANSACTION_REPLAY_REPLY_INFO);
                LOGGER.info("I am proc {}, I will send transactions replay reply, between cid {} --> {}", tomLayer.getCurrentProcessId(), cid, cid + batchSize -1);
                tomLayer.getCommunication().send(trReplyMessage, sender);
                cid = cid + batchSize;
            } else {
                TransactionReplayState state = getReplayState(cid,  endCid);
                trReplyMessage = new StandardTRMessage(topology.getCurrentProcessId(), sender, state, cid, endCid, TOMUtil.SM_TRANSACTION_REPLAY_REPLY_INFO);
                LOGGER.info("I am proc {}, I will send transactions replay reply, between cid {} --> {}", tomLayer.getCurrentProcessId(), cid, endCid);
                tomLayer.getCommunication().send(trReplyMessage, sender);
                break;
            }
        }

    }

    private TransactionReplayState getReplayState(int startCid, int endCid) {

        LOGGER.info("I am proc {}, I will getReplayState between cid {} --> {}!", tomLayer.getCurrentProcessId(), startCid, endCid);

        CommandsInfo[] commandsInfos = new CommandsInfo[endCid - startCid + 1];

        for (int i = 0, cid = startCid; (i <= endCid - startCid) && (cid <= endCid); i++, cid++) {

            DefaultRecoverable recoverable = (DefaultRecoverable)(tomLayer.getDeliveryThread().getRecoverer());
            commandsInfos[i] =  new CommandsInfo();
            commandsInfos[i].commands = recoverable.getCommandsByCid(cid,  recoverable.getCommandsNumByCid(cid));

            MessageContext[] msgCtxs = new MessageContext[commandsInfos[i].commands.length];

            for (int index = 0; index < commandsInfos[i].commands.length; index++) {
                msgCtxs[index] = new MessageContext(0, 0, null, 0, 0, 0, 0, null, recoverable.getTimestampByCid(cid), 0, 0, 0, 0, cid, null, null, false);
            }
            commandsInfos[i].msgCtx = msgCtxs;
        }

        return new DefaultTransactionReplayState(commandsInfos, startCid, endCid, topology.getCurrentProcessId());
    }

    private void saveValidDataSenders(HashMap<Integer,Integer> senderCIDs, int key) {
        for (int replicaId : senderCIDs.keySet()) {
            if (senderCIDs.get(replicaId) == key) {
                validDataSenders.put(new Integer(replicaId) ,new Integer(key));
            }
        }
    }

    private void checkViewInfo(int currViewId, HashMap<Integer, Integer> senderVids) {
        for (int sender : senderVids.keySet()) {
            if (currViewId < senderVids.get(sender)) {
                LOGGER.info("#############################################################################################################################################");
                LOGGER.info("################Check View Info, Current View Is Obsolete, If Block Exist Diff, Please Copy Ledger Database And Restart!#####################");
                LOGGER.info("#############################################################################################################################################");
                break;
            }
        }
    }

    protected abstract void requestState();

    @Override
    public abstract void stateTimeout();

    @Override
    public abstract void SMRequestDeliver(SMMessage msg, boolean isBFT);

    @Override
    public abstract void SMReplyDeliver(SMMessage msg, boolean isBFT);

}
