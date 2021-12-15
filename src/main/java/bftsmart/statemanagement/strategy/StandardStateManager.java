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

import bftsmart.consensus.Consensus;
import bftsmart.consensus.Epoch;
import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.consensus.messages.MessageFactory;
import bftsmart.reconfiguration.views.NodeNetwork;
import bftsmart.reconfiguration.views.View;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.SMMessage;
import bftsmart.tom.core.DeliveryThread;
import bftsmart.tom.core.ExecutionManager;
import bftsmart.tom.core.TOMLayer;
import bftsmart.tom.leaderchange.CertifiedDecision;
import bftsmart.tom.util.TOMUtil;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 
 * @author Marcel Santos
 *
 */
public class StandardStateManager extends BaseStateManager {

    private int replica;
    private ReentrantLock lockTimer = new ReentrantLock();
    private Timer stateTimer = null;

    //private LCManager lcManager;
    private ExecutionManager execManager;

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(StandardStateManager.class);


    @Override
    public void init(TOMLayer tomLayer, DeliveryThread dt) {
    	topology = tomLayer.controller;
    	
        this.tomLayer = tomLayer;
        this.dt = dt;
        //this.lcManager = tomLayer.getSyncher().getLCManager();
        this.execManager = tomLayer.execManager;

        changeReplica(); // initialize replica from which to ask the complete state
                
        //this.replica = 0;
        //if (SVController.getCurrentViewN() > 1 && replica == SVController.getStaticConf().getProcessId())
        //	changeReplica();

        state = null;
        lastCID = -1;
        waitingCID = -1;

        appStateOnly = false;
    }

    public TOMLayer getTomLayer() {
        return this.tomLayer;
    }

    private void changeReplica() {
        
        int[] processes = this.topology.getCurrentViewOtherAcceptors();
        Random r = new Random();
        
        int pos;
        do {
            //pos = this.SVController.getCurrentViewPos(replica);
            //replica = this.SVController.getCurrentViewProcesses()[(pos + 1) % SVController.getCurrentViewN()];
            
            if (processes != null && processes.length > 1) {
                pos = r.nextInt(processes.length);
                replica = processes[pos];
            } else {
                replica = 0;
                break;
            }
        } while (replica == topology.getStaticConf().getProcessId());
    }
    
    @Override
    protected void requestState() {
        if (!doWork) {
            return;
        }
        if (tomLayer.requestsTimer != null)
        	tomLayer.requestsTimer.clearAll();

        // 优化时待处理
//        changeReplica(); // always ask the complete state to a different replica
        
        SMMessage smsg = new StandardSMMessage(topology.getStaticConf().getProcessId(),
                waitingCID, TOMUtil.SM_REQUEST, replica, null, null, -1, -1);
        tomLayer.getCommunication().send(topology.getCurrentViewOtherAcceptors(), smsg);

        LOGGER.info("(StandardStateManager.requestState) I just sent a request to the other replicas for the state up to CID {}", waitingCID);

        TimerTask stateTask =  new TimerTask() {
            public void run() {
                if (!doWork) {
                    return;
                }
            	LOGGER.info("Timeout to retrieve state");
                int[] myself = new int[1];
                myself[0] = topology.getStaticConf().getProcessId();
                tomLayer.getCommunication().send(myself, new StandardSMMessage(-1, waitingCID, TOMUtil.TRIGGER_SM_LOCALLY, -1, null, null, -1, -1));
            }
        };

        stateTimer = new Timer("state timer");
        timeout = timeout * 2;
        stateTimer.schedule(stateTask,timeout);
    }

    @Override
    public void stateTimeout() {
        lockTimer.lock();
        LOGGER.info("(StateManager.stateTimeout) Timeout for the replica that was supposed to send the complete state. Changing desired replica.");
        LOGGER.info("Timeout no timer do estado!");
        if (stateTimer != null)
        	stateTimer.cancel();
        changeReplica();
        reset();
        requestState();
        lockTimer.unlock();
    }

    public void shutdown() {
        LOGGER.info("I will shut down StandardStateManager !");
        doWork = false;
        isInitializing = true;
        if (stateTimer != null) {
            stateTimer.cancel();
        }
        if (replayTimer != null) {
            replayTimer.cancel();
        }
    }
    
	@Override
    public void SMRequestDeliver(SMMessage msg, boolean isBFT) {

        LOGGER.info("I will handle SMRequestDeliver !");

        if (!tomLayer.isLastCidSetOk()) {
            LOGGER.info("I am proc {}, ignore SMMessage msg, wait tomlayer set last cid!", tomLayer.getCurrentProcessId());
            return;
        }

        try {
            if (topology.getStaticConf().isStateTransferEnabled() && dt.getRecoverer() != null) {
                StandardSMMessage stdMsg = (StandardSMMessage)msg;
    //            boolean sendState = stdMsg.getReplica() == SVController.getStaticConf().getProcessId();

                // 目前都发送状态，防止被请求的节点恰好是区块落后，或者是重新启动的节点而没有状态；
                boolean sendState = true;

                LOGGER.info("-- Should I send the state? {}", sendState);

                ApplicationState thisState = dt.getRecoverer().getState(msg.getCID(), sendState);
                if (thisState == null) {

                    LOGGER.info("-- For some reason, I am sending a void state");
                  thisState = dt.getRecoverer().getState(-1, sendState);
                }
                else {
                    LOGGER.info("-- Will I send the state? {}", thisState.getSerializedState() != null);
                }
                int[] targets = { msg.getSender() };
                SMMessage smsg = new StandardSMMessage(topology.getStaticConf().getProcessId(),
                        msg.getCID(), TOMUtil.SM_REPLY, -1, thisState, topology.getCurrentView(),
                        tomLayer.getSynchronizer().getLCManager().getLastReg(), tomLayer.execManager.getCurrentLeader());

                LOGGER.info("Sending state");
                tomLayer.getCommunication().send(targets, smsg);
                LOGGER.info("Sent");
            }
        } catch (Exception e) {
            LOGGER.error("[StandardStateManager] SMRequestDeliver exception! {}", e.getMessage());
        }
    }

	@Override
    public void SMReplyDeliver(SMMessage msg, boolean isBFT) {
        LOGGER.info("I will handle SMReplyDeliver !");
        lockTimer.lock();
        if (topology.getStaticConf().isStateTransferEnabled()) {
            if (waitingCID != -1 && msg.getCID() == waitingCID) {
                int currentRegency = -1;
                int currentLeader = -1;
                View currentView = null;
                CertifiedDecision currentProof = null;
                
                if (!appStateOnly) {
                	//TODO: 未正确更新 regency 和 leader；
//                	senderRegencies.put(msg.getSender(), msg.getRegency());
//                	senderLeaders.put(msg.getSender(), msg.getLeader());
                	senderViews.put(msg.getSender(), msg.getView());
                    senderProofs.put(msg.getSender(), msg.getState().getCertifiedDecision(topology));
                    senderStates.put(msg.getSender(), msg.getState());
//                    if (enoughRegencies(msg.getRegency())) currentRegency = msg.getRegency();
//                    if (enoughLeaders(msg.getLeader())) currentLeader = msg.getLeader();
                    if (enoughViews(msg.getView())) currentView = msg.getView();
                    if (enoughProofs(waitingCID, this.tomLayer.getSynchronizer().getLCManager())) currentProof = msg.getState().getCertifiedDecision(topology);
                    if (enoughState(msg.getState())) state = msg.getState();
                    
                } else {
                    currentLeader = tomLayer.execManager.getCurrentLeader();
                    currentRegency = tomLayer.getSynchronizer().getLCManager().getLastReg();
                    currentView = topology.getCurrentView();
                }

                // 待优化处理
//                if (msg.getSender() == replica && msg.getState().getSerializedState() != null) {
//                	LOGGER.info("Expected replica sent state. Setting it to state");
//                    state = msg.getState();
//                    if (stateTimer != null) stateTimer.cancel();
//                }

                LOGGER.info("Verifying more than Quorum consistent replies");
                if (state != null) {
                    LOGGER.info("More than Quorum consistent confirmed");
                    ApplicationState otherReplicaState = getOtherReplicaState();
                    LOGGER.info("State != null: {}, recvState != null: {}",(state != null), (otherReplicaState != null));
                    int haveState = 0;
//                        if(state != null) {
                            byte[] hash = null;
                            hash = tomLayer.computeHash(state.getSerializedState());
                            if (otherReplicaState != null) {
                                if (Arrays.equals(hash, otherReplicaState.getStateHash())) haveState = 1;
                                else if (getNumEqualStates() > topology.getCurrentViewF())
                                    haveState = -1;
                            }
//                        }

                    LOGGER.debug("haveState: {}", haveState);
                                            
//                    if (otherReplicaState != null && haveState == 1 && currentRegency > -1 &&
//                            //此行将导致currentProof==null时无法进行下面的处理；
////                            currentLeader > -1 && currentView != null && (!isBFT || currentProof != null || appStateOnly)) {
//                            currentLeader > -1 && currentView != null ) {

                    if (otherReplicaState != null && haveState == 1 && currentView != null ) {

                    	LOGGER.debug("Received state. Will install it");

                    	// 由leader confirm 任务去做更改leader, regency的事情
//                    	if (currentRegency > tomLayer.getSynchronizer().getLCManager().getLastReg()) {
//                    		tomLayer.getSynchronizer().getLCManager().jumpToRegency(new LeaderRegency(currentLeader, currentRegency));
//                    		tomLayer.execManager.setNewLeader(currentLeader);
//						}
                        
                        if (currentProof != null && !appStateOnly) {
                            
                            LOGGER.debug("Installing proof for consensus {}", waitingCID);

                            Consensus cons = execManager.getConsensus(waitingCID);
                            Epoch e = null;
                            
                            for (ConsensusMessage cm : currentProof.getConsMessages()) {

                                e = cons.getEpoch(cm.getEpoch(), true, topology);
                                if (e.getTimestamp() != cm.getEpoch()) {

                                    LOGGER.debug("Strange... proof contains messages from more than just one epoch");
                                    e = cons.getEpoch(cm.getEpoch(), true, topology);
                                }
                                e.addToProof(cm);

                                if (cm.getType() == MessageFactory.ACCEPT) {
                                    e.setAccept(cm.getSender(), cm.getValue());
                                }

                                else if (cm.getType() == MessageFactory.WRITE) {
                                    e.setWrite(cm.getSender(), cm.getValue());
                                }

                            }
                            
                            
                            if (e != null) {

                                hash = tomLayer.computeHash(currentProof.getDecision());
                                e.propValueHash = hash;
                                e.propValue = currentProof.getDecision();
                                e.deserializedPropValue = tomLayer.checkProposedValue(currentProof.getDecision(), false);
                                 cons.decided(e, false);
                                 
                                LOGGER.debug("Successfully installed proof for consensus {}", waitingCID);

                            } else {
                                LOGGER.error("Failed to install proof for consensus {}", waitingCID);

                            }

                        }
                    
                        // I might have timed out before invoking the state transfer, so
                        // stop my re-transmission of STOP messages for all regencies up to the current one
//                        if (currentRegency > 0) tomLayer.getSynchronizer().removeSTOPretransmissions(currentRegency - 1);
                        //if (currentRegency > 0)
                        //    tomLayer.requestsTimer.setTimeout(tomLayer.requestsTimer.getTimeout() * (currentRegency * 2));
                        
                        dt.deliverLock();
                        waitingCID = -1;
                        dt.update(state);
                        
//                        if (!appStateOnly && execManager.stopped()) {
//                            Queue<ConsensusMessage> stoppedMsgs = execManager.getStoppedMsgs();
//                            for (ConsensusMessage stopped : stoppedMsgs) {
//                                if (stopped.getNumber() > state.getLastCID() /*msg.getCID()*/)
//                                    execManager.addOutOfContextMessage(stopped);
//                            }
//                            execManager.clearStopped();
//                            execManager.restart();
//                        }
                        
//                        tomLayer.processOutOfContext();
                        
                        if (topology.getCurrentViewId() <= currentView.getId()) {
                            LOGGER.info("Installing current view!");
                            // 当通过交易重放回补落后区块时，不仅要更新本地视图，还需要同时更新本地的hostconfig
                            topology.reconfigureTo(currentView);
                            updateHostConfig(currentView);
                        }
                        
						isInitializing = false;

                        // trigger out of context propose msg process
                        tomLayer.processOutOfContext();

                        dt.canDeliver();
                        dt.deliverUnlock();

                        reset();

                        LOGGER.info("I updated the state!");
                        // 有差异的话，完成后会走此流程
                        tomLayer.connectRemotesOK();

//                        tomLayer.requestsTimer.Enabled(true);
//                        tomLayer.requestsTimer.startTimer();
                        if (stateTimer != null) stateTimer.cancel();
                        
                        if (appStateOnly) {
                        	appStateOnly = false;
                            tomLayer.getSynchronizer().resumeLC();
                        }
                    } else if (otherReplicaState == null && (topology.getCurrentViewN() / 2) < getReplies()) {
                    	LOGGER.info("otherReplicaState == null && (SVController.getCurrentViewN() / 2) < getReplies()");
                        waitingCID = -1;
                        reset();
 
                        if (stateTimer != null) stateTimer.cancel();
                        
                        if (appStateOnly) {
                            requestState();
                        }
                    } else if (haveState == -1) {
                        LOGGER.error("haveState == -1");
                        LOGGER.error("(TOMLayer.SMReplyDeliver) The replica from which I expected the state, sent one which doesn't match the hash of the others, or it never sent it at all");

                        changeReplica();
                        reset();
                        requestState();

                        if (stateTimer != null) stateTimer.cancel();
                    } else if (haveState == 0 && (topology.getCurrentViewN() - topology.getCurrentViewF()) <= getReplies()) {

                        LOGGER.error("(TOMLayer.SMReplyDeliver) Could not obtain the state, retrying");
                        reset();
                        if (stateTimer != null) stateTimer.cancel();
                        waitingCID = -1;
                        //requestState();
                    } else {
                        LOGGER.debug(" -- State transfer not yet finished");

                    }
                }
            }
        }
        lockTimer.unlock();
    }

    private void updateHostConfig(View currentView) {
        LOGGER.info("State transfer, update host config!");
        for (int procId : currentView.getProcesses()) {
            NodeNetwork nodeNetwork = currentView.getAddress(procId);
            if (nodeNetwork != null) {
            	this.topology.addHostInfo(procId, nodeNetwork.getHost(), nodeNetwork.getConsensusPort(), nodeNetwork.getMonitorPort(), nodeNetwork.isConsensusSecure(), nodeNetwork.isMonitorSecure());
            } else {
                LOGGER.info("updateHostConfig, find node network is null!");
            }
        }
    }

    /**
     * Search in the received states table for a state that was not sent by the expected
     * replica. This is used to compare both states after received the state from expected
     * and other replicas.
     * @return The state sent from other replica
     */
    private ApplicationState getOtherReplicaState() {
    	int[] processes = topology.getCurrentViewProcesses();
    	for(int process : processes) {
    		if(process == replica)
    			continue;
    		else {
    			ApplicationState otherState = senderStates.get(process);
    			if(otherState != null)
    				return otherState;
    		}
    	}
    	return null;
    }

    private int getNumEqualStates() {
    	List<ApplicationState> states = new ArrayList<ApplicationState>(receivedStates()); 
    	int match = 0;
        for (ApplicationState st1 : states) {
        	int count = 0;
            for (ApplicationState st2 : states) {
            	if(st1 != null && st1.equals(st2))
            		count++;
            }
            if(count > match)
            	match = count;
        }
        return match;
    }

    @Override
	public void currentConsensusIdAsked(int sender, int viewId) {
        LOGGER.info("I am proc {}, I will handle currentConsensusIdAsked sender = {}!", tomLayer.getCurrentProcessId(), sender);

        // 用来保证本地共识ID，以及tom config 文件配置完成
        if (!tomLayer.isLastCidSetOk()) {
            LOGGER.info("I am proc {}, ignore request cid msg, wait tomlayer set last cid!", tomLayer.getCurrentProcessId());
            return;
        }

        if (viewId < this.topology.getCurrentView().getId()) {
            LOGGER.info("#######################################################################################################################################################");
            LOGGER.info("################State Transfer Requester View Is Obsolete, If Block Exist Diff, Please Requester Copy Ledger Database And Restart!#####################");
            LOGGER.info("#######################################################################################################################################################");
        }
		int me = topology.getCurrentProcessId();
		int lastConsensusId = tomLayer.getLastExec();
		LOGGER.info("I am proc {}, will send consensusId = {} !", tomLayer.getCurrentProcessId(), lastConsensusId);
		SMMessage currentCID = new StandardSMMessage(me, lastConsensusId, TOMUtil.SM_REPLY_INITIAL, 0, null, this.topology.getCurrentView(), 0, 0);
		tomLayer.getCommunication().send(new int[]{sender}, currentCID);
	}
	
}
