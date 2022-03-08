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
package bftsmart.statemanagement.strategy.durability;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantLock;

import bftsmart.statemanagement.strategy.StandardTRMessage;
import org.slf4j.LoggerFactory;

import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.reconfiguration.views.NodeNetwork;
import bftsmart.reconfiguration.views.View;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.SMMessage;
import bftsmart.statemanagement.strategy.BaseStateManager;
import bftsmart.tom.core.DeliveryThread;
import bftsmart.tom.core.ExecutionManager;
import bftsmart.tom.core.TOMLayer;
import bftsmart.tom.leaderchange.LeaderRegency;
import bftsmart.tom.server.defaultservices.CommandsInfo;
import bftsmart.tom.server.defaultservices.durability.DurabilityCoordinator;
import bftsmart.tom.util.TOMUtil;

public class DurableStateManager extends BaseStateManager {

	// private LCManager lcManager;
	private ExecutionManager execManager;

	private ReentrantLock lockTimer = new ReentrantLock();
	private Timer stateTimer = null;
	private final static long INIT_TIMEOUT = 40000;
	private long timeout = INIT_TIMEOUT;

	private CSTRequestF1 cstRequest;

	private CSTState stateCkp;
	private CSTState stateLower;
	private CSTState stateUpper;

	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(DurableStateManager.class);

	@Override
	public void init(TOMLayer tomLayer, DeliveryThread dt) {
		topology = tomLayer.controller;

		this.tomLayer = tomLayer;
		this.dt = dt;
		// this.lcManager = tomLayer.getSyncher().getLCManager();
		this.execManager = tomLayer.execManager;

		state = null;
		lastCID = 1;
		waitingCID = -1;

		appStateOnly = false;
	}

	@Override
	protected void requestState() {
		if (tomLayer.requestsTimer != null)
			tomLayer.requestsTimer.clearAll();

		int myProcessId = topology.getStaticConf().getProcessId();
		int[] otherProcesses = topology.getCurrentViewOtherAcceptors();
		int globalCkpPeriod = topology.getStaticConf().getGlobalCheckpointPeriod();

		CSTRequestF1 cst = new CSTRequestF1(waitingCID);
		cst.defineReplicas(otherProcesses, globalCkpPeriod, myProcessId);
		this.cstRequest = cst;
		CSTSMMessage cstMsg = new CSTSMMessage(myProcessId, waitingCID, TOMUtil.SM_REQUEST, cst, null, null, -1, -1);
		tomLayer.getCommunication().send(topology.getCurrentViewOtherAcceptors(), cstMsg);

		LOGGER.info("(TOMLayer.requestState) I just sent a request to the other replicas for the state up to CID {}",
				waitingCID);

		TimerTask stateTask = new TimerTask() {
			public void run() {
				int[] myself = new int[1];
				myself[0] = topology.getStaticConf().getProcessId();
				tomLayer.getCommunication().send(myself,
						new CSTSMMessage(-1, waitingCID, TOMUtil.TRIGGER_SM_LOCALLY, null, null, null, -1, -1));
			}
		};

		stateTimer = new Timer("state timer");
		timeout = timeout * 2;
		stateTimer.schedule(stateTask, timeout);
	}

	@Override
	public void stateTimeout() {
		lockTimer.lock();
		LOGGER.info(
				"(StateManager.stateTimeout) Timeout for the replica that was supposed to send the complete state. Changing desired replica.");
		LOGGER.info("Timeout no timer do estado!");
		if (stateTimer != null)
			stateTimer.cancel();
		reset();
		requestState();
		lockTimer.unlock();
	}

	@Override
	public void SMRequestDeliver(SMMessage msg, boolean isBFT) {
		LOGGER.debug("(TOMLayer.SMRequestDeliver) invoked method");
		if (topology.getStaticConf().isStateTransferEnabled() && dt.getRecoverer() != null) {
			LOGGER.debug("(TOMLayer.SMRequestDeliver) The state transfer protocol is enabled");
			LOGGER.info("(TOMLayer.SMRequestDeliver) I received a state request for CID {} from replica {}",
					msg.getCID(), msg.getSender());
			CSTSMMessage cstMsg = (CSTSMMessage) msg;
			CSTRequestF1 cstConfig = cstMsg.getCstConfig();
			boolean sendState = cstConfig.getCheckpointReplica() == topology.getStaticConf().getProcessId();
			if (sendState)
				LOGGER.info("(TOMLayer.SMRequestDeliver) I should be the one sending the state");

			LOGGER.debug("--- state asked");

			int[] targets = { msg.getSender() };
			NodeNetwork address = topology.getCurrentView().getAddress(topology.getStaticConf().getProcessId());
			String myIp = address.getHost();
			int myId = topology.getStaticConf().getProcessId();
			int port = 4444 + myId;
			address = new NodeNetwork(myIp, port, -1, address.isConsensusSecure(), false);
			cstConfig.setAddress(address);
			CSTSMMessage reply = new CSTSMMessage(myId, msg.getCID(), TOMUtil.SM_REPLY, cstConfig, null,
					topology.getCurrentView(), tomLayer.getSynchronizer().getLCManager().getLastReg(),
					tomLayer.execManager.getCurrentLeader());

			StateSenderServer stateServer = new StateSenderServer(port);
			stateServer.setRecoverable(dt.getRecoverer());
			stateServer.setRequest(cstConfig);
			new Thread(stateServer).start();

			tomLayer.getCommunication().send(targets, reply);

		}
	}

	@Override
	public void SMReplyDeliver(SMMessage msg, boolean isBFT) {
		lockTimer.lock();
		CSTSMMessage reply = (CSTSMMessage) msg;
		if (topology.getStaticConf().isStateTransferEnabled()) {
			LOGGER.debug("(TOMLayer.SMReplyDeliver) The state transfer protocol is enabled");
			LOGGER.info("(TOMLayer.SMReplyDeliver) I received a state reply for CID {} from replica {}", reply.getCID(),
					reply.getSender());

			LOGGER.info("--- Received CID: {}, Waiting CID: {}", reply.getCID(), waitingCID);
			if (waitingCID != -1 && reply.getCID() == waitingCID) {

				int currentRegency = -1;
				int currentLeader = -1;
				View currentView = null;
				// CertifiedDecision currentProof = null;

				if (!appStateOnly) {
					senderRegencies.put(reply.getSender(), reply.getRegency());
					senderLeaders.put(reply.getSender(), reply.getLeader());
					senderViews.put(reply.getSender(), reply.getView());
					// senderProofs.put(msg.getSender(),
					// msg.getState().getCertifiedDecision(SVController));
					if (enoughRegencies(reply.getRegency()))
						currentRegency = reply.getRegency();
					if (enoughLeaders(reply.getLeader()))
						currentLeader = reply.getLeader();
					if (enoughViews(reply.getView())) {
						currentView = reply.getView();
						if (!currentView.isMember(topology.getStaticConf().getProcessId())) {
							LOGGER.error("Not a member!");
						}
					}
					// if (enoughProofs(waitingCID, this.tomLayer.getSynchronizer().getLCManager()))
					// currentProof = msg.getState().getCertifiedDecision(SVController);

				} else {
					currentLeader = tomLayer.execManager.getCurrentLeader();
					currentRegency = tomLayer.getSynchronizer().getLCManager().getLastReg();
					currentView = topology.getCurrentView();
				}

				LOGGER.info("(TOMLayer.SMReplyDeliver) The reply is for the CID that I want!");

				NodeNetwork address = reply.getCstConfig().getAddress();
				Socket clientSocket;
				ApplicationState stateReceived = null;
				try {
					clientSocket = new Socket(address.getHost(), address.getConsensusPort());
					ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());
					stateReceived = (ApplicationState) in.readObject();
				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				if (stateReceived instanceof CSTState) {
					senderStates.put(reply.getSender(), stateReceived);
					if (reply.getSender() == cstRequest.getCheckpointReplica())
						this.stateCkp = (CSTState) stateReceived;
					if (reply.getSender() == cstRequest.getLogLower())
						this.stateLower = (CSTState) stateReceived;
					if (reply.getSender() == cstRequest.getLogUpper())
						this.stateUpper = (CSTState) stateReceived;
				}

				if (senderStates.size() == 3) {

					CommandsInfo[] lowerLog = stateLower.getLogLower();
					CommandsInfo[] upperLog = stateUpper.getLogUpper();
					LOGGER.debug("lowerLog ");
					if (lowerLog != null)
						LOGGER.debug("Lower log length size: {} ", lowerLog.length);
					LOGGER.debug("upperLog ");
					if (upperLog != null)
						LOGGER.debug("Upper log length size: {} ", upperLog.length);

					boolean haveState = false;
					byte[] lowerbytes = TOMUtil.getBytes(lowerLog);
					LOGGER.debug("Log lower bytes size: {}", lowerbytes.length);
					byte[] upperbytes = TOMUtil.getBytes(upperLog);
					LOGGER.debug("Log upper bytes size: {}", upperbytes.length);

					byte[] lowerLogHash = new byte[0];
					byte[] upperLogHash = new byte[0];

					try {
						lowerLogHash = TOMUtil.computeHash(lowerbytes);
						upperLogHash = TOMUtil.computeHash(upperbytes);
					} catch (NoSuchAlgorithmException e) {
						e.printStackTrace();
					}

					// validate lower log
					if (Arrays.equals(stateCkp.getHashLogLower(), lowerLogHash))
						haveState = true;
					else
						LOGGER.error("Lower log don't match");
					// validate upper log
					if (!haveState || !Arrays.equals(stateCkp.getHashLogUpper(), upperLogHash)) {
						haveState = false;
						LOGGER.error("Upper log don't match");
					}

					CSTState statePlusLower = new CSTState(stateCkp.getSerializedState(),
							TOMUtil.getBytes(stateCkp.getSerializedState()), stateLower.getLogLower(),
							stateCkp.getHashLogLower(), null, null, stateCkp.getCheckpointCID(),
							stateUpper.getCheckpointCID(), topology.getStaticConf().getProcessId());

					if (haveState) { // validate checkpoint
						LOGGER.debug("validating checkpoint!!!");
						dt.getRecoverer().setState(statePlusLower);
						byte[] currentStateHash = ((DurabilityCoordinator) dt.getRecoverer()).getCurrentStateHash();
						if (!Arrays.equals(currentStateHash, stateUpper.getHashCheckpoint())) {
							LOGGER.error("ckp hash don't match");
							haveState = false;
						}
					}

					LOGGER.info("-- current regency: {} ", currentRegency);
					LOGGER.info("-- current leader: {}", currentLeader);
					LOGGER.info("-- current view: {}", currentView);
					if (currentRegency > -1 && currentLeader > -1 && currentView != null && haveState
							&& (!isBFT || /* currentProof != null || */ appStateOnly)) {
						LOGGER.info("---- RECEIVED VALID STATE ----");

						LOGGER.info("(TOMLayer.SMReplyDeliver) The state of those replies is good!");
						LOGGER.info("(TOMLayer.SMReplyDeliver) CID State requested: {}", reply.getCID());
						LOGGER.info("(TOMLayer.SMReplyDeliver) CID State received: {}", stateUpper.getLastCID());

						if (currentRegency > tomLayer.getSynchronizer().getLCManager().getCurrentRegency().getId()) {
							tomLayer.getSynchronizer().getLCManager()
									.jumpToRegency(new LeaderRegency(currentLeader, currentRegency));
							tomLayer.execManager.setNewLeader(currentLeader);
						}

//						if (currentProof != null && !appStateOnly) {
//
//							LOGGER.debug("Installing proof for consensus " + waitingCID);
//
//							Consensus cons = execManager.getConsensus(waitingCID);
//							Epoch e = null;
//
//							for (ConsensusMessage cm : currentProof.getConsMessages()) {
//
//								e = cons.getEpoch(cm.getEpoch(), true, SVController);
//								if (e.getTimestamp() != cm.getEpoch()) {
//
//									LOGGER.debug("Strange... proof contains messages from more than just one epoch");
//									e = cons.getEpoch(cm.getEpoch(), true, SVController);
//								}
//								e.addToProof(cm);
//
//								if (cm.getType() == MessageFactory.ACCEPT) {
//									e.setAccept(cm.getSender(), cm.getValue());
//								}
//
//								else if (cm.getType() == MessageFactory.WRITE) {
//									e.setWrite(cm.getSender(), cm.getValue());
//								}
//
//							}
//
//
//							if (e != null) {
//
//								byte[] hash = tomLayer.computeHash(currentProof.getDecision());
//								e.propValueHash = hash;
//								e.propValue = currentProof.getDecision();
//								e.deserializedPropValue = tomLayer.checkProposedValue(currentProof.getDecision(), false);
//								cons.decided(e, false);
//
//								LOGGER.debug("Successfully installed proof for consensus " + waitingCID);
//
//							} else {
//								LOGGER.debug("Failed to install proof for consensus " + waitingCID);
//
//							}
//
//						}

						// I might have timed out before invoking the state transfer, so
						// stop my re-transmission of STOP messages for all regencies up to the current
						// one
						if (currentRegency > 0)
							tomLayer.getSynchronizer().removeSTOPretransmissions(currentRegency - 1);

						LOGGER.debug("trying to acquire deliverlock");
						dt.deliverLock();
						LOGGER.debug("acquired");

						// this makes the isRetrievingState() evaluates to false
						waitingCID = -1;
						dt.update(stateUpper);

						// Deal with stopped messages that may come from
						// synchronization phase
						if (!appStateOnly && execManager.stopped()) {
							Queue<ConsensusMessage> stoppedMsgs = execManager.getStoppedMsgs();
							for (ConsensusMessage stopped : stoppedMsgs) {
								if (stopped.getNumber() > state.getLastCID())
									execManager.addOutOfContextMessage(stopped);
							}
							execManager.clearStopped();
							execManager.restart();
						}

						LOGGER.info("Processing out of context messages");

						tomLayer.processOutOfContext();

						if (topology.getCurrentViewId() != currentView.getId()) {
							LOGGER.info("Installing current view!");
							topology.reconfigureTo(currentView);
						}

						isInitializing = false;

						dt.canDeliver();
						dt.deliverUnlock();

						reset();

						LOGGER.info("I updated the state!");

//						tomLayer.requestsTimer.Enabled(true);
//						tomLayer.requestsTimer.startTimer();
						if (stateTimer != null)
							stateTimer.cancel();

						if (appStateOnly) {
							appStateOnly = false;
							tomLayer.getSynchronizer().resumeLC();
						}
					} else if (state == null && (topology.getCurrentViewN() / 2) < getReplies()) {
						LOGGER.error("---- DIDNT RECEIVE STATE ----");

						LOGGER.error("(TOMLayer.SMReplyDeliver) I have more than {} messages that are no good!",
								(topology.getCurrentViewN() / 2));

						waitingCID = -1;
						reset();

						if (stateTimer != null)
							stateTimer.cancel();

						if (appStateOnly) {
							requestState();
						}
					} else if (!haveState) {
						LOGGER.error("---- RECEIVED INVALID STATE  ----");

						LOGGER.error(
								"(TOMLayer.SMReplyDeliver) The replica from which I expected the state, sent one which doesn't match the hash of the others, or it never sent it at all");

						reset();
						requestState();

						if (stateTimer != null)
							stateTimer.cancel();
					}
				}
			}
		}
		lockTimer.unlock();
	}

	@Override
	public void askTransactionReplay(int startCid, int endCid) {

	}

	@Override
	public void transactionReplayAsked(int sender, int target, int startCid, int endCid) {

	}

	@Override
	public void transactionReplayReplyDeliver(StandardTRMessage msg) {

	}

}
