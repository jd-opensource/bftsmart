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
package bftsmart.tom.core;

import java.io.IOException;
import java.io.Serializable;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.SignedObject;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.clientsmanagement.ClientsManager;
import bftsmart.clientsmanagement.RequestList;
import bftsmart.communication.ServerCommunicationSystem;
import bftsmart.communication.client.RequestReceiver;
import bftsmart.consensus.Consensus;
import bftsmart.consensus.Decision;
import bftsmart.consensus.Epoch;
import bftsmart.consensus.app.SHA256Utils;
import bftsmart.consensus.roles.Acceptor;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.statemanagement.StateManager;
import bftsmart.statemanagement.strategy.StandardStateManager;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.core.messages.ForwardedMessage;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.leaderchange.ClientDatasMonitorTimer;
import bftsmart.tom.leaderchange.HeartBeatTimer;
import bftsmart.tom.leaderchange.RequestsTimer;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.server.RequestVerifier;
import bftsmart.tom.server.defaultservices.DefaultRecoverable;
import bftsmart.tom.util.BatchBuilder;
import bftsmart.tom.util.BatchReader;

/**
 * This class implements the state machine replication protocol described in
 * Joao Sousa's 'From Byzantine Consensus to BFT state machine replication: a
 * latency-optimal transformation' (May 2012)
 * 
 * The synchronization phase described in the paper is implemented in the
 * Synchronizer class
 */
public class TOMLayer extends Thread implements RequestReceiver {

	private static final Logger LOGGER = LoggerFactory.getLogger(TOMLayer.class);

	private volatile boolean doWork = true;
	// other components used by the TOMLayer (they are never changed)
	public ExecutionManager execManager; // Execution manager
	public Acceptor acceptor; // Acceptor role of the PaW algorithm
	private ServerCommunicationSystem communication; // Communication system between replicas
	// private OutOfContextMessageThread ot; // Thread which manages messages that
	// do not belong to the current consensus
	private DeliveryThread dt; // Thread which delivers total ordered messages to the appication
	public StateManager stateManager = null; // object which deals with the state transfer protocol

	private volatile boolean isConnectRemotesOK = false;

	private volatile boolean isLastCidSetOk = false;

	private volatile boolean isLeaderConfirmed = false;

	/**
	 * Manage timers for pending requests
	 */
	public final RequestsTimer requestsTimer;

	public final HeartBeatTimer heartBeatTimer;

	public final ViewSyncTimer viewSyncTimer;

	// Monitor timer for client datas and clear too old datas;
	public ClientDatasMonitorTimer clientDatasMonitorTimer;
	/**
	 * Store requests received but still not ordered
	 */
	public ClientsManager clientsManager;
	/**
	 * The id of the consensus being executed (or -1 if there is none)
	 */
	private int inExecution = -1;
	private int lastExecuted = -1;

	public SHA256Utils md = new SHA256Utils();
	private Signature engine;

	private ReentrantLock hashLock = new ReentrantLock();

	// the next two are used to generate non-deterministic data in a deterministic
	// way (by the leader)
	public BatchBuilder bb = new BatchBuilder(System.nanoTime());

	/* The locks and conditions used to wait upon creating a propose */
	private ReentrantLock leaderLock = new ReentrantLock();
	private Condition iAmLeader = leaderLock.newCondition();
	private ReentrantLock messagesLock = new ReentrantLock();
	private Condition haveMessages = messagesLock.newCondition();
	private ReentrantLock proposeLock = new ReentrantLock();
	private Condition canPropose = proposeLock.newCondition();

	private PrivateKey prk;
	public ServerViewController controller;

//	private ServiceReplica receiver;

	private RequestVerifier verifier;

	private Synchronizer syncher;

	private String realName;

	/**
	 * Creates a new instance of TOMulticastLayer
	 *
	 * @param manager    Execution manager
	 * @param receiver   Object that receives requests from clients
	 * @param recoverer
	 * @param a          Acceptor role of the PaW algorithm
	 * @param cs         Communication system between replicas
	 * @param controller Reconfiguration Manager
	 * @param verifier
	 */
	public TOMLayer(ExecutionManager manager, ServiceReplica receiver, Recoverable recoverer, Acceptor a,
			ServerCommunicationSystem cs, ServerViewController controller, RequestVerifier verifier) {
		super("TOM Layer");

		this.execManager = manager;
//		this.receiver = receiver;
		this.acceptor = a;
		this.communication = cs;
		this.controller = controller;
		this.requestsTimer = new RequestsTimer(this, communication, this.controller); // Create requests timers manager
																						// (a thread)
		this.heartBeatTimer = new HeartBeatTimer(this);
		this.viewSyncTimer = new ViewSyncTimer(this);
		// do not create a timer manager if the timeout is 0
//        if (this.controller.getStaticConf().getRequestTimeout() == 0) {
//            this.requestsTimer = null;
//        } else {
//        }

//        try {
//            this.md = MessageDigest.getInstance("MD5"); // TODO: shouldn't it be SHA?
//        } catch (Exception e) {
//            e.printStackTrace(System.out);
//        }

		try {
			this.engine = Signature.getInstance("SHA1withRSA");
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}

		this.prk = this.controller.getStaticConf().getRSAPrivateKey();
		this.dt = new DeliveryThread(this, receiver, recoverer, this.controller); // Create delivery thread
		this.dt.start();
		this.stateManager = recoverer.getStateManager();
		stateManager.init(this, dt);

		this.verifier = (verifier != null) ? verifier : new RequestVerifier() {
			@Override
			public boolean isValidRequest(byte[] request) {
				return true; // By default, never validate requests
			}
		};

		// I have a verifier, now create clients manager
		this.clientsManager = new ClientsManager(this.controller, requestsTimer, this.verifier);

		this.clientDatasMonitorTimer = new ClientDatasMonitorTimer(this.controller, requestsTimer, clientsManager);

		this.syncher = new Synchronizer(this); // create synchronizer
	}

	public boolean isRunning() {
		return doWork;
	}

	public int getCurrentProcessId() {
		return controller.getStaticConf().getProcessId();
	}

	/**
	 * Computes an hash for a TOM message
	 *
	 * @param data Data from which to generate the hash
	 * @return Hash for the specified TOM message
	 */
	public final byte[] computeHash(byte[] data) {
		byte[] ret = null;
		hashLock.lock();
		try {
			ret = md.hash(data);
		} finally {
			hashLock.unlock();
		}

		return ret;
	}

	public SignedObject sign(Serializable obj) {
		try {
			return new SignedObject(obj, prk, engine);
		} catch (InvalidKeyException | SignatureException | IOException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	/**
	 * Verifies the signature of a signed object
	 *
	 * @param so     Signed object to be verified
	 * @param sender Replica id that supposedly signed this object
	 * @return True if the signature is valid, false otherwise
	 */
	public boolean verifySignature(SignedObject so, int sender) {
		try {
			return so.verify(controller.getStaticConf().getRSAPublicKey(sender), engine);
		} catch (InvalidKeyException | SignatureException e) {
			LOGGER.warn("Invalid key or signature! --" + e.getMessage(), e);
		}
		return false;
	}

	/**
	 * Retrieve Communication system between replicas
	 *
	 * @return Communication system between replicas
	 */
	public ServerCommunicationSystem getCommunication() {
		return this.communication;
	}

	public void imAmTheLeader() {
		leaderLock.lock();
		iAmLeader.signal();
		leaderLock.unlock();
	}

	/**
	 * Sets which consensus was the last to be executed
	 *
	 * @param last ID of the consensus which was last to be executed
	 */
	public void setLastExec(int last) {
		this.lastExecuted = last;
	}

	/**
	 * Gets the ID of the consensus which was established as the last executed
	 *
	 * @return ID of the consensus which was established as the last executed
	 */
	public int getLastExec() {
		return this.lastExecuted;
	}

	/**
	 * Sets which consensus is being executed at the moment
	 *
	 * @param inEx ID of the consensus being executed at the moment
	 */
	public void setInExec(int inEx) {
		proposeLock.lock();
		LOGGER.debug("(TOMLayer.setInExec) modifying inExec from {} to {}", this.inExecution, inEx);
		this.inExecution = inEx;
		if (inEx == -1 && !isRetrievingState()) {
			canPropose.signalAll();
		}
		proposeLock.unlock();
	}

	/**
	 * This method blocks until the PaW algorithm is finished
	 */
	public void waitForPaxosToFinish() {
		proposeLock.lock();
		canPropose.awaitUninterruptibly();
		proposeLock.unlock();
	}

	/**
	 * Gets the ID of the consensus currently beign executed
	 *
	 * @return ID of the consensus currently beign executed (if no consensus ir
	 *         executing, -1 is returned)
	 */
	public int getInExec() {
		return this.inExecution;
	}

	/**
	 * This method is invoked by the communication system to deliver a request. It
	 * assumes that the communication system delivers the message in FIFO order.
	 *
	 * @param msg The request being received
	 */
	@Override
	public void requestReceived(TOMMessage msg) {

		if (!doWork)
			return;

		// check if this request is valid and add it to the client' pending requests
		// list
		boolean readOnly = (msg.getReqType() == TOMMessageType.UNORDERED_REQUEST
				|| msg.getReqType() == TOMMessageType.UNORDERED_HASHED_REQUEST);
		if (readOnly) {
			LOGGER.debug(
					"I am {}, (TOMLayer.requestReceived) Received read-only TOMMessage from client {} with sequence number {} for session {}",
					controller.getStaticConf().getProcessId(), msg.getSender(), msg.getSequence(), msg.getSession());

			dt.deliverUnordered(msg, syncher.getLCManager().getLastReg());
		} else {
			if(LOGGER.isDebugEnabled()) {
				LOGGER.debug(
						"(TOMLayer.requestReceived) Received TOMMessage from client {} with sequence number {} for session {}",
						msg.getSender(), msg.getSequence(), msg.getSession());
			}
			if (clientsManager.requestReceived(msg, true, communication)) {
				haveMessages();
			} else {
				LOGGER.error("(TOMLayer.requestReceived) the received TOMMessage {}  was discarded.", msg);
			}
		}
	}

	private DefaultRecoverable getDefaultExecutor() {
		return (DefaultRecoverable) getDeliveryThread().getReceiver().getExecutor();
	}

	/**
	 * 注释掉无调用的逻辑；2020-12-28 by huanghaiquan;
	 * 
	 * private void reply(TOMMessage message) { if
	 * (controller.getStaticConf().getNumRepliers() > 0) { LOGGER.debug(
	 * "(ServiceReplica.receiveMessages) sending reply to {} with sequence number {}
	 * and operation ID {} via ReplyManager", message.getSender(),
	 * message.getSequence(), message.getOperationId());
	 * receiver.getRepMan().send(message); } else { LOGGER.debug(
	 * "(ServiceReplica.receiveMessages) sending reply to {} with sequence number {}
	 * and operation ID {}", message.getSender(), message.getSequence(),
	 * message.getOperationId()); receiver.getReplier().manageReply(message, null);
	 * } }
	 */

	/**
	 * Creates a value to be proposed to the acceptors. Invoked if this replica is
	 * the leader
	 *
	 * @param dec Object that will eventually hold the decided value
	 * @return A value to be proposed to the acceptors
	 */
	public byte[] createPropose(Decision dec) {
		// Retrieve a set of pending requests from the clients manager
		RequestList pendingRequests = clientsManager.getPendingRequests();

		int numberOfMessages = pendingRequests.size(); // number of messages retrieved
		int numberOfNonces = this.controller.getStaticConf().getNumberOfNonces(); // ammount of nonces to be generated

		// for benchmarking
		if (dec.getConsensusId() > -1) { // if this is from the leader change, it doesnt matter
			dec.firstMessageProposed = pendingRequests.getFirst();
			dec.firstMessageProposed.consensusStartTime = System.nanoTime();
		}
		dec.batchSize = numberOfMessages;

		LOGGER.debug("(TOMLayer.run) creating a PROPOSE with {} msgs", numberOfMessages);

		return bb.makeBatch(pendingRequests, numberOfNonces, System.currentTimeMillis(), controller);
	}

	public String getRealName() {
		return realName;
	}

	public void setRealName(String realName) {
		this.realName = realName;
	}

	/**
	 * 是否是Leader
	 * 
	 * @return
	 */
	public boolean isLeader() {
		return leader() == this.controller.getStaticConf().getProcessId();
	}

	/**
	 * 是否连接远端节点正常
	 *
	 * @return
	 */
	public boolean isConnectRemotesOK() {
		return isConnectRemotesOK;
	}

	/**
	 * 连接远端成功
	 *
	 */
	public void connectRemotesOK() {
		this.isConnectRemotesOK = true;
	}

	/**
	 * 等待状态传输结束
	 *
	 */
	public void waitConnectRemotesOK() {
		this.isConnectRemotesOK = false;
	}


	public boolean isLastCidSetOk() {
		return isLastCidSetOk;
	}

	public void lastCidSetOk() {
		this.isLastCidSetOk = true;
	}

	public boolean isLeaderConfirmed() {
		return isLeaderConfirmed;
	}

	public void setLeaderConfirmed(boolean leaderConfirmed) {
		isLeaderConfirmed = leaderConfirmed;
	}

	/**
	 * leader对应ID
	 * 
	 * @return
	 */
	public int leader() {
		return execManager.getCurrentLeader();
	}

	/**
	 * This is the main code for this thread. It basically waits until this replica
	 * becomes the leader, and when so, proposes a value to the other acceptors
	 */
	@Override
	public void run() {

		try {
			LOGGER.debug("TomLayer start running."); // TODO: can't this be outside of the loop?
			this.heartBeatTimer.start();
			this.viewSyncTimer.start();
			while (doWork) {
				try {
					if (isLeaderConfirmed()) {
						doEpoch();
					}
				} catch (Exception e) {
					LOGGER.error("Error occurred while doing epoch! --[CurrentProcessId=" + this.getCurrentProcessId()
							+ "] " + e.getMessage(), e);
				}
			}
		} catch (Throwable e) {
			LOGGER.error("Error occurred and interupted the epoch loop of TOMLayer! --[CurrentProcessId="
					+ this.getCurrentProcessId() + "] " + e.getMessage(), e);
		}

		LOGGER.info("TOMLayer stopped.");
	}

	private void doEpoch() {
		// blocks until this replica learns to be the leader for the current epoch of
		// the current consensus
		leaderLock.lock();
		LOGGER.debug("Next leader for CID {} : {}", (getLastExec() + 1), execManager.getCurrentLeader());

		// ******* EDUARDO BEGIN **************//
		if (execManager.getCurrentLeader() != this.controller.getStaticConf().getProcessId()) {
			iAmLeader.awaitUninterruptibly();
			// waitForPaxosToFinish();
		}
		// ******* EDUARDO END **************//
		leaderLock.unlock();

		if (!doWork) {
			return;
		}

		// blocks until the current consensus finishes
		proposeLock.lock();

		if (getInExec() != -1) { // there is some consensus running
			LOGGER.debug("(TOMLayer.run) Waiting for consensus {} termination.", getInExec());
			canPropose.awaitUninterruptibly();
		}
		proposeLock.unlock();

		if (!doWork) {
			return;
		}

		LOGGER.debug("(TOMLayer.run) I'm the leader.");

		// blocks until there are requests to be processed/ordered
		messagesLock.lock();
		if (!clientsManager.havePendingRequests()) {
			haveMessages.awaitUninterruptibly();
		}
		messagesLock.unlock();

		if (!doWork) {
			return;
		}

		LOGGER.debug("(TOMLayer.run) There are messages to be ordered.");

		LOGGER.info("(TOMLayer.run) I am proc {}, I can try to propose.",
				this.controller.getStaticConf().getProcessId());

		if ((execManager.getCurrentLeader() == controller.getCurrentProcessId()) && // I'm the leader
				(clientsManager.havePendingRequests()) && // there are messages to be ordered
				(getInExec() == -1)) { // there is no consensus in execution

			// Sets the current consensus
			int execId = getLastExec() + 1;
			setInExec(execId);

			Decision dec = execManager.getConsensus(execId).getDecision();

			// Bypass protocol if service is not replicated
			if (controller.getCurrentViewN() == 1) {
				LOGGER.debug("(TOMLayer.run) Only one replica, bypassing consensus.");

				byte[] value = createPropose(dec);

				Consensus consensus = execManager.getConsensus(dec.getConsensusId());
				Epoch epoch = consensus.getEpoch(0, controller);
				epoch.propValue = value;
				epoch.propValueHash = computeHash(value);
				epoch.getConsensus().addWritten(value);
				epoch.deserializedPropValue = checkProposedValue(value, true);
				epoch.getConsensus().getDecision().firstMessageProposed = epoch.deserializedPropValue[0];
				dec.setDecisionEpoch(epoch);

				// System.out.println("ESTOU AQUI!");
				dt.delivery(dec);
			} else {
				byte[] value = createPropose(dec);

				Consensus consensus = execManager.getConsensus(dec.getConsensusId());
				// create epoch;
				Epoch epoch = consensus.getEpoch(0, controller);
//                    epoch.propValue = value;
//                    epoch.propValueHash = computeHash(value);
				LOGGER.debug("Propose new consensus by leader! --[Leader={}][ConsensusId={}]",
						controller.getCurrentProcessId(), consensus.getId());
				execManager.getProposer().startConsensus(consensus.getId(), value);
			}

			if(controller.getStaticConf().getEpochDelay() > 0) {
				try {
					sleep(controller.getStaticConf().getEpochDelay());
				} catch (InterruptedException e) {
					LOGGER.error("Epoch delay interrupted", e);
				}
			}
		}
	}

	/**
	 * Called by the current consensus instance, to notify the TOM layer that a
	 * value was decided
	 *
	 * @param dec The decision of the consensus
	 */
	public void decided(Decision dec) {

		dec.setRegency(syncher.getLCManager().getLastReg());
		dec.setLeader(execManager.getCurrentLeader());

		this.dt.delivery(dec); // Sends the decision to the delivery thread
	}

	/**
	 * Verify if the value being proposed for a epoch is valid. It verifies the
	 * client signature of all batch requests.
	 *
	 * TODO: verify timestamps and nonces
	 *
	 * @param proposedValue the value being proposed
	 * @return Valid messages contained in the proposed value
	 */
	public TOMMessage[] checkProposedValue(byte[] proposedValue, boolean addToClientManager) {
		LOGGER.debug("(TOMLayer.isProposedValueValid) starting");

		BatchReader batchReader = new BatchReader(proposedValue,
				this.controller.getStaticConf().isUseSignatures());

		TOMMessage[] requests = null;

		try {

			// deserialize the message
			// TODO: verify Timestamps and Nonces
			requests = batchReader.deserialiseRequests(this.controller);

			// enforce the "external validity" property, i.e, verify if the
			// requests are valid in accordance to the application semantics
			// and not an erroneous requests sent by a Byzantine leader.
			for (TOMMessage r : requests) {
				if (controller.getStaticConf().isBFT() && !verifier.isValidRequest(r.getContent()))
					return null;
			}

			if (addToClientManager) {
				for (int i = 0; i < requests.length; i++) {
					// notifies the client manager that this request was received and get
					// the result of its validation
					if (!clientsManager.requestReceived(requests[i], false)) {
						clientsManager.getClientsLock().unlock();
						LOGGER.error("(TOMLayer.isProposedValueValid) finished, return=false");
						LOGGER.error("failure in deserialize batch");
						return null;
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			clientsManager.getClientsLock().unlock();
			LOGGER.error("(TOMLayer.isProposedValueValid) finished, return=false");
			return null;
		}
		LOGGER.debug("(TOMLayer.isProposedValueValid) finished, return=true");

		return requests;
	}

	public void forwardRequestToLeader(TOMMessage request) {
		int leaderId = execManager.getCurrentLeader();
		if (this.controller.isCurrentViewMember(leaderId)) {
			LOGGER.debug("(TOMLayer.forwardRequestToLeader) forwarding {} to {}", request, leaderId);
			communication.send(new int[] { leaderId },
					new ForwardedMessage(this.controller.getStaticConf().getProcessId(), request));
		}
	}

	public boolean isReady() {
		return (!stateManager.isRetrievingState()) && (heartBeatTimer.isActived());
	}

	public boolean isRetrievingState() {
		// lockTimer.lock();
		boolean result = stateManager != null && stateManager.isRetrievingState();
		// lockTimer.unlock();

		return result;
	}

	public void setNoExec() {
		LOGGER.debug("(TOMLayer.setNoExec) modifying inExec from {} to -1", this.inExecution);

		proposeLock.lock();
		this.inExecution = -1;
		// ot.addUpdate();
		canPropose.signalAll();
		proposeLock.unlock();
	}

	public void processOutOfContext() {
		// 状态传输以及LC未完成，不进行超预期消息的处理
		if (!this.isReady()) {
			return;
		}

		for (int nextConsensus = getLastExec() + 1; execManager
				.receivedOutOfContextPropose(nextConsensus); nextConsensus = getLastExec() + 1) {
			execManager.processOutOfContextPropose(execManager.getConsensus(nextConsensus));
		}
	}

	public void processOutOfContextWriteAndAccept() {
		// 状态传输以及LC未完成，不进行超预期消息的处理
		if (!this.isReady()) {
			return;
		}

		// 对于一轮共识，如果propose消息未处理，则暂不处理write , accept消息；getInExec() == nextConsensus用来保证先处理propose类型消息
		for (int nextConsensus = getLastExec() + 1; ((execManager.receivedOutOfContextWriteAndAccept(nextConsensus)) && (getInExec() == nextConsensus));nextConsensus = getLastExec() + 1) {
			execManager.processOutOfContextWriteAndAccept(execManager.getConsensus(nextConsensus));
		}
	}

	public StateManager getStateManager() {
		return stateManager;
	}

	public Synchronizer getSynchronizer() {
		return syncher;
	}

	private void haveMessages() {
		messagesLock.lock();
		haveMessages.signal();
		messagesLock.unlock();
	}

	public DeliveryThread getDeliveryThread() {
		return dt;
	}

	public ExecutionManager getExecManager() {
		return execManager;
	}

	public void shutdown() {
		if (!doWork) {
			// 已经关闭，不必重复执行；
			return;
		}
		// 先关闭心跳定时器；
		// 如果当前节点是领导者，那么当心跳定时器关闭前会主动发送 STOP 消息通知其它节点触发选举；
		if (this.heartBeatTimer != null) {
			this.heartBeatTimer.shutdown();
		}

		this.doWork = false;
		imAmTheLeader();
		haveMessages();
		setNoExec();

		if (viewSyncTimer != null) {
			viewSyncTimer.shutdown();
		}

		if (stateManager != null && stateManager instanceof StandardStateManager) {
			StandardStateManager standardStateManager = (StandardStateManager) stateManager;
			standardStateManager.shutdown();
		}

		if (this.clientDatasMonitorTimer != null) {
			this.clientDatasMonitorTimer.shutdown();
		}
		if (this.requestsTimer != null)
			this.requestsTimer.shutdown();
		if (this.clientsManager != null) {
			this.clientsManager.clear();
			this.clientsManager.getPendingRequests().clear();
		}
		if (this.dt != null)
			this.dt.shutdown();
		if (this.communication != null)
			this.communication.shutdown();

	}

	public HeartBeatTimer getHeartBeatTimer() {
		return heartBeatTimer;
	}

	public RequestsTimer getRequestsTimer() {
		return requestsTimer;
	}
}
