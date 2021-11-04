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
import java.util.Set;

import bftsmart.statemanagement.TRMessage;
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
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(BaseStateManager.class);

    protected boolean appStateOnly;
    protected volatile int waitingCID = -1;
    protected int lastCID;
    protected int lastLogCid;
    protected ApplicationState state;

    protected volatile boolean isInitializing = true;

    protected volatile boolean doWork = true;

    protected int STATETRANSFER_RETRY_COUNT = 500;

    private HashMap<Integer, Integer> senderCIDs = null;

    private HashMap<Integer, Integer> senderVIDs = null;

    private List<Integer> validDataSenders = new ArrayList<>();

    public BaseStateManager() {
        senderStates = new HashMap<>();
        senderViews = new HashMap<>();
        senderRegencies = new HashMap<>();
        senderLeaders = new HashMap<>();
        senderProofs = new HashMap<>();
    }

    public List<Integer> getValidDataSenders() {return validDataSenders;}

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
        for (CertifiedDecision cDec : senderProofs.values()) {
                                    
            if (cDec != null && cid == proofIsConsistent(cDec.getConsMessages()) && lc.hasValidProof(cDec)) {
                counter++;
            }
            
        }
        boolean result = counter >= topology.getQuorum();
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
        validDataSenders.clear();
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
       LOGGER.info("(TOMLayer.analyzeState) The state transfer protocol is enabled");
        if (waitingCID == -1) {
           LOGGER.info("(TOMLayer.analyzeState) I'm not waiting for any state, so I will keep record of this message");
            if (tomLayer.execManager.isDecidable(cid)) {
                LOGGER.info("BaseStateManager.analyzeState: I have now more than {} messages for CID {} which are beyond CID {}", topology.getCurrentViewF(), cid, lastCID);
                lastCID = cid;
                waitingCID = cid - 1;
                LOGGER.info("analyzeState {}", waitingCID);
                requestState();
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
            LOGGER.info("I will send StandardSMMessage[{}] to others !", TOMUtil.SM_ASK_INITIAL);
            tomLayer.getCommunication().send(target, currentCID);
            try {
                Thread.sleep(5000);
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
        LOGGER.info("I will handle currentConsensusIdReceived!");
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

        LOGGER.info("currentConsensusIdReceived ->sender[{}], CID = {} !", smsg.getSender(), smsg.getCID());
        LOGGER.info("senderCIDs.size() = {}, and SVController.getQuorum()= {} !", senderCIDs.size(), topology.getQuorum());
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

                    saveValidDataSenders(senderCIDs, key);
                    // 根据提供状态同步节点的key，获得其检查点信息
                    int checkPointFormOtherNode = this.tomLayer.controller.getStaticConf().getCheckpointPeriod() * (key / this.tomLayer.controller.getStaticConf().getCheckpointPeriod()) - 1;

                    if (lastCID >= key) {
                        LOGGER.info("-- {} replica state is up to date ! --", topology.getStaticConf().getProcessId());
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
                        LOGGER.info("-- Requesting state from other replicas, key = {}, lastCid = {}", key, lastCID);
//                        this.lastLogCid = lastCID;
                        lastCID = key + 1;
                        if (waitingCID == -1) {
                            waitingCID = key;
                            requestState();
                        }
                    }
                }
            }
        }
    }

    private void saveValidDataSenders(HashMap<Integer,Integer> senderCIDs, int key) {
        for (int replicaId : senderCIDs.keySet()) {
            if (senderCIDs.get(replicaId) == key) {
                validDataSenders.add(new Integer(replicaId));
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

    @Override
    public void transactionReplayReceived(TRMessage msg) {

    }

    protected abstract void requestState();

    @Override
    public abstract void stateTimeout();

    @Override
    public abstract void SMRequestDeliver(SMMessage msg, boolean isBFT);

    @Override
    public abstract void SMReplyDeliver(SMMessage msg, boolean isBFT);

}
