/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.tom.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.SignedObject;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.LoggerFactory;

import bftsmart.communication.ServerCommunicationSystem;
import bftsmart.consensus.Consensus;
import bftsmart.consensus.Decision;
import bftsmart.consensus.Epoch;
import bftsmart.consensus.TimestampValuePair;
import bftsmart.consensus.app.SHA256Utils;
import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.consensus.messages.MessageFactory;
import bftsmart.consensus.roles.Acceptor;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.reconfiguration.views.View;
import bftsmart.statemanagement.StateManager;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.leaderchange.CertifiedDecision;
import bftsmart.tom.leaderchange.CollectData;
import bftsmart.tom.leaderchange.ElectionResult;
import bftsmart.tom.leaderchange.LeaderRegencyPropose;
import bftsmart.tom.leaderchange.HeartBeatTimer;
import bftsmart.tom.leaderchange.LCManager;
import bftsmart.tom.leaderchange.LCMessage;
import bftsmart.tom.leaderchange.LCType;
import bftsmart.tom.leaderchange.RequestsTimer;
import bftsmart.tom.server.defaultservices.DefaultRecoverable;
import bftsmart.tom.util.BatchBuilder;
import bftsmart.tom.util.BatchReader;
import bftsmart.tom.util.TOMUtil;

/**
 *
 * This class implements the synchronization phase described in Joao Sousa's
 * 'From Byzantine Consensus to BFT state machine replication: a latency-optimal
 * transformation' (May 2012)
 * 
 * This class implements all optimizations described at the end of the paper
 * 
 * @author joao
 */
public class Synchronizer {

	// out of context messages related to the leader change are stored here
	private final Set<LCMessage> outOfContextLC;

	// Manager of the leader change
	private final LCManager lcManager;

	// Total order layer
	private final TOMLayer tom;

	// Stuff from TOMLayer that this object needs
	private final RequestsTimer requestsTimer;
	private final HeartBeatTimer heartBeatTimer;
	private final ExecutionManager execManager;
	private final ServerViewController controller;
	private final BatchBuilder bb;
	private final ServerCommunicationSystem communication;
	private final StateManager stateManager;
	private final Acceptor acceptor;
	private SHA256Utils md = new SHA256Utils();

	// Attributes to temporarely store synchronization info
	// if state transfer is required for synchronization
	private int tempRegency = -1;
	private CertifiedDecision tempLastHighestCID = null;
	private HashSet<SignedObject> tempSignedCollects = null;
	private byte[] tempPropose = null;
	private int tempBatchSize = -1;
	private boolean tempIAmLeader = false;

	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Synchronizer.class);

	public Synchronizer(TOMLayer tom) {

		this.tom = tom;

		this.requestsTimer = this.tom.requestsTimer;
		this.heartBeatTimer = this.tom.heartBeatTimer;
		this.execManager = this.tom.execManager;
		this.controller = this.tom.controller;
		this.bb = this.tom.bb;
		this.communication = this.tom.getCommunication();
		this.stateManager = this.tom.stateManager;
		this.acceptor = this.tom.acceptor;
		this.md = this.tom.md;

		this.outOfContextLC = Collections.synchronizedSet(new HashSet<>());
		this.lcManager = new LCManager(this.tom, this.controller, this.md);
	}

	public LCManager getLCManager() {
		return lcManager;
	}

	/**
	 * This method is called when there is a timeout and the request has already
	 * been forwarded to the leader
	 *
	 * @param requestList List of requests that the replica wanted to order but
	 *                    didn't manage to
	 */
	public void triggerTimeout(LeaderRegencyPropose regencyPropose, List<TOMMessage> requestList) {

		ObjectOutputStream out = null;
		ByteArrayOutputStream bos = new ByteArrayOutputStream();

//		int regency = lcManager.getNextReg();

//		heartBeatTimer.stopAll();
//        requestsTimer.Enabled(false);

		// still not in the leader change phase?
		int proposedNewRegency = regencyPropose.getRegency().getId();
		if (lcManager.tryBeginElection(regencyPropose.getRegency())) {
			heartBeatTimer.stopAll();

//		if (!lcManager.isElecting()) {
//			lcManager.setNextReg(lcManager.getLastReg() + 1); // define next timestamp

//			int regency = lcManager.getNextReg(); // update variable

			// store messages to be ordered
			lcManager.setCurrentRequestTimedOut(requestList);

			// 当前领导者；
			final int currentLeader = tom.getExecManager().getCurrentLeader();

			// store information about messages that I'm going to send
			// 加入当前节点的执政期选举提议；
//			lcManager.addStop(currentLeader, proposedNewRegency, this.controller.getStaticConf().getProcessId());
			lcManager.addStop(regencyPropose);

			// execManager.stop(); // stop consensus execution

			// Get requests that timed out and the requests received in STOP messages
			// and add those STOPed requests to the client manager
			addSTOPedRequestsToClientManager();
			List<TOMMessage> messages = getRequestsToRelay();

			try { // serialize content to send in STOP message
				out = new ObjectOutputStream(bos);

				if (messages != null && messages.size() > 0) {

					// TODO: If this is null, then there was no timeout nor STOP messages.
					// What to do?
					byte[] serialized = bb.makeBatch(messages, 0, 0, controller);
					out.writeBoolean(true);
					out.writeObject(serialized);
				} else {
					out.writeBoolean(false);

					LOGGER.debug(
							"(Synchronizer.triggerTimeout) [{}] -> I am proc {} Strange... did not include any request in my STOP message for regency {}",
							this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
							proposedNewRegency);
				}

				byte[] payload = bos.toByteArray();

				out.flush();
				bos.flush();

				out.close();
				bos.close();

				// send STOP-message
				LOGGER.info(
						"(Synchronizer.triggerTimeout) [{}] -> I am proc {} sending STOP message to install regency {}, with {} request(s) to relay",
						this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
						proposedNewRegency, (messages != null ? messages.size() : 0));

				LCMessage msgSTOP = LCMessage.createSTOP(this.controller.getStaticConf().getProcessId(),
						regencyPropose.getRegency(), this.controller.getCurrentView(), payload);
				requestsTimer.setSTOP(proposedNewRegency, msgSTOP); // make replica re-transmit the stop message until a
																	// new regency
				// is installed
				communication.send(this.controller.getCurrentViewOtherAcceptors(), msgSTOP);

			} catch (IOException ex) {
				ex.printStackTrace();
				java.util.logging.Logger.getLogger(TOMLayer.class.getName()).log(Level.SEVERE, null, ex);
			} finally {
				try {
					out.close();
					bos.close();
				} catch (IOException ex) {
					ex.printStackTrace();
					java.util.logging.Logger.getLogger(TOMLayer.class.getName()).log(Level.SEVERE, null, ex);
				}
			}

		}

		processOutOfContextSTOPs(proposedNewRegency); // the replica might have received STOPs
		// that were out of context at the time they
		// were received, but now can be processed

		startSynchronization(regencyPropose, false); // evaluate STOP messages

	}

	// Processes STOP messages that were not process upon reception, because they
	// were
	// ahead of the replica's expected regency
	private void processOutOfContextSTOPs(int regency) {

//        Logger.println("(Synchronizer.processOutOfContextSTOPs) Checking if there are out of context STOPs for regency " + regency);

		Set<LCMessage> stops = getOutOfContextLC(LCType.STOP, regency);

		if (stops.size() > 0) {
			LOGGER.info(
					"(Synchronizer.processOutOfContextSTOPs) [{}] -> I am proc {} Processing {} out of context STOPs for regency {}",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
					stops.size(), regency);
		} else {
			LOGGER.info(
					"(Synchronizer.processOutOfContextSTOPs) [{}] -> I am proc {} No out of context STOPs for regency {}",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(), regency);
		}

		for (LCMessage m : stops) {
			TOMMessage[] requests = deserializeTOMMessages(m.getPayload());

			// store requests that came with the STOP message
			lcManager.addRequestsFromSTOP(requests);

			// store information about the STOP message
			LeaderRegencyPropose propose = LeaderRegencyPropose.copy(m.getLeader(), m.getReg(), m.getViewId(),
					m.getViewProcessIds(), m.getSender());
			lcManager.addStop(propose);
		}
	}

	// Processes STOPDATA messages that were not process upon reception, because
	// they were
	// ahead of the replica's expected regency
	private void processSTOPDATA(LCMessage msg, int regency) {
		aa
		// TODO: It is necessary to verify the proof of the last decided consensus and
		// the signature of the state of the current consensus!
		CertifiedDecision lastData = null;
		SignedObject signedCollect = null;

		int last = -1;
		byte[] lastValue = null;
		Set<ConsensusMessage> proof = null;

		ByteArrayInputStream bis;
		ObjectInputStream ois;

		LeaderRegencyPropose regencyPropose = LeaderRegencyPropose.copy(msg.getLeader(), msg.getReg(), msg.getViewId(),
				msg.getViewProcessIds(), msg.getSender());
		try { // deserialize the content of the message

			bis = new ByteArrayInputStream(msg.getPayload());
			ois = new ObjectInputStream(bis);

			if (ois.readBoolean()) { // content of the last decided cid

				last = ois.readInt();

				lastValue = (byte[]) ois.readObject();
				proof = (Set<ConsensusMessage>) ois.readObject();

				// TODO: Proof is missing!
			}

			lastData = new CertifiedDecision(msg.getSender(), last, lastValue, proof);

			lcManager.addLastCID(regency, lastData);

			signedCollect = (SignedObject) ois.readObject();

			ois.close();
			bis.close();

			lcManager.addCollect(regency, signedCollect);

			int bizantineQuorum = (controller.getCurrentViewN() + controller.getCurrentViewF()) / 2;
			int cftQuorum = (controller.getCurrentViewN()) / 2;

			// Did I already got messages from a Byzantine/Crash quorum,
			// related to the last cid as well as for the current?
			boolean conditionBFT = (controller.getStaticConf().isBFT()
					&& lcManager.getLastCIDsSize(regency) > bizantineQuorum
					&& lcManager.getCollectsSize(regency) > bizantineQuorum);

			boolean conditionCFT = (lcManager.getLastCIDsSize(regency) > cftQuorum
					&& lcManager.getCollectsSize(regency) > cftQuorum);

			if (conditionBFT || conditionCFT) {
				LOGGER.info(
						"(Synchronizer.processSTOPDATA) [{}] -> I am proc {}, I recv >= 3 StopData, I will catch up regency {}, from proc {}, from port {}",
						this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
						regency, msg.getSender(),
						controller.getStaticConf().getRemoteAddress(msg.getSender()).getConsensusPort());
				catch_up(regency);
			}

		} catch (IOException ex) {
			ex.printStackTrace(System.err);
		} catch (ClassNotFoundException ex) {
			ex.printStackTrace(System.err);
		}

	}

	// Processes SYNC messages that were not process upon reception, because they
	// were
	// ahead of the replica's expected regency
	private void processSYNC(byte[] payload, int regency) {

		CertifiedDecision lastHighestCID = null;
		int currentCID = -1;
		HashSet<SignedObject> signedCollects = null;
		byte[] propose = null;
		int batchSize = -1;

		ByteArrayInputStream bis;
		ObjectInputStream ois;

		try { // deserialization of the message content

			bis = new ByteArrayInputStream(payload);
			ois = new ObjectInputStream(bis);

			lastHighestCID = (CertifiedDecision) ois.readObject();
			signedCollects = (HashSet<SignedObject>) ois.readObject();
			propose = (byte[]) ois.readObject();
			batchSize = ois.readInt();

			lcManager.setCollects(regency, signedCollects);

			currentCID = lastHighestCID.getCID() + 1;

			// Is the predicate "sound" true? Is the certificate for LastCID valid?
			if (lcManager.sound(lcManager.selectCollects(regency, currentCID))
					&& (!controller.getStaticConf().isBFT() || lcManager.hasValidProof(lastHighestCID))) {
				LOGGER.info("(Synchronizer.processSYNC) [{}] -> I am proc {}, sound succ , I will finalise",
						this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId());
				finalise(regency, lastHighestCID, signedCollects, propose, batchSize, false);
			}

			ois.close();
			bis.close();

		} catch (IOException ex) {
			ex.printStackTrace();
			java.util.logging.Logger.getLogger(TOMLayer.class.getName()).log(Level.SEVERE, null, ex);
		} catch (ClassNotFoundException ex) {
			ex.printStackTrace();
			java.util.logging.Logger.getLogger(TOMLayer.class.getName()).log(Level.SEVERE, null, ex);

		}
	}

	/**
	 * Fetches synchronization messages that were not process upon reception,
	 * because they were ahead of the replica's expected regency
	 * 
	 * <p>
	 * 返回指定执政期的指定类型的超前抵达的消息；
	 * 
	 * @param type
	 * @param regency
	 * @return
	 */
	private Set<LCMessage> getOutOfContextLC(LCType type, int regency) {

		HashSet<LCMessage> result = new HashSet<>();

		for (LCMessage m : outOfContextLC) {

			if (m.getType() == type && m.getReg() == regency) {
				result.add(m);
			}

		}

		outOfContextLC.removeAll(result); // avoid memory leaks

		return result;
	}

	// Deserializes requests that were included in STOP messages
	private TOMMessage[] deserializeTOMMessages(byte[] playload) {

		ByteArrayInputStream bis = null;
		ObjectInputStream ois = null;

		TOMMessage[] requests = null;

		try { // deserialize the content of the STOP message

			bis = new ByteArrayInputStream(playload);
			ois = new ObjectInputStream(bis);

			boolean hasReqs = ois.readBoolean();

			if (hasReqs) {

				// Store requests that the other replica did not manage to order
				// TODO: The requests have to be verified!
				byte[] temp = (byte[]) ois.readObject();
				BatchReader batchReader = new BatchReader(temp, controller.getStaticConf().getUseSignatures() == 1);
				requests = batchReader.deserialiseRequests(controller);
			}

			ois.close();
			bis.close();

		} catch (EOFException ex) {
			return requests;
		} catch (IOException ex) {
			ex.printStackTrace();
			java.util.logging.Logger.getLogger(TOMLayer.class.getName()).log(Level.SEVERE, null, ex);
		} catch (ClassNotFoundException ex) {
			ex.printStackTrace();
			java.util.logging.Logger.getLogger(TOMLayer.class.getName()).log(Level.SEVERE, null, ex);

		}

		return requests;

	}

	// Get requests that timed out and the requests received in STOP messages
	private List<TOMMessage> getRequestsToRelay() {

		List<TOMMessage> messages = lcManager.getCurrentRequestTimedOut();

		if (messages == null) {

			messages = new LinkedList<>();
		}

		// Include requests from STOP messages in my own STOP message
		List<TOMMessage> messagesFromSTOP = lcManager.getRequestsFromSTOP();
		if (messagesFromSTOP != null) {

			for (TOMMessage m : messagesFromSTOP) {

				if (!messages.contains(m)) {

					messages.add(m);
				}
			}
		}

		LOGGER.debug("(Synchronizer.getRequestsToRelay) [{}] -> I need to relay {} requests",
				this.execManager.getTOMLayer().getRealName(), messages.size());

		return messages;
	}

	// adds requests received via STOP messages to the client manager
	private void addSTOPedRequestsToClientManager() {

		List<TOMMessage> messagesFromSTOP = lcManager.getRequestsFromSTOP();
		if (messagesFromSTOP != null) {

			LOGGER.debug(
					"(Synchronizer.addRequestsToClientManager) [{}] -> Adding to client manager the requests contained in STOP messages",
					this.execManager.getTOMLayer().getRealName());

			for (TOMMessage m : messagesFromSTOP) {
				tom.requestReceived(m);

			}
		}

	}

	/**
	 * Remove all STOP messages being retransmitted up until the specified regency
	 * 
	 * @param regency The regency up to which STOP retransmission should be canceled
	 */
	public void removeSTOPretransmissions(int regency) {

		Set<Integer> timers = requestsTimer.getTimers();

		for (int t : timers) {
			if (t <= regency)
				requestsTimer.stopSTOP(t);
		}

	}

	// this method is called when a timeout occurs or when a STOP message is
	// recevied
	private synchronized void startSynchronization(LeaderRegencyPropose regencyPropose, boolean triggerBySTOP) {
		LOGGER.info("(Synchronizer.startSynchronization) [{}] -> I am proc {}, initialize synchr phase",
				this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId());

		// Ask to start the synchronizations phase if enough messages have been received
		// already

		final int proposedRegencyId = regencyPropose.getRegency().getId();

		// 此处可能会 Follower 的心跳检测超时处理线程形成竞争进入选举进程；
		// 如果此时已经处于选举进程，则不作后续处理；
		if ((!lcManager.isInProgress()) && lcManager.isUpToBeginQuorum(proposedRegencyId)) {
			broadcast_LC_STOP(regencyPropose, triggerBySTOP);
		}

		// Did the synchronization phase really started?

		// LCManager有两种状态：
		// 1: 进入“选举中”状态；
		// 2：在本次“选举周期”下并发地收到其它节点的 STOPDATA 消息，完成了本次轮选举；
		ElectionResult electionResult = lcManager.generateQuorumElectionResult(proposedRegencyId);
		if (lcManager.isInProgress(proposedRegencyId) && electionResult != null) {
			if (!execManager.stopped()) {
				execManager.stop(); // stop consensus execution if more than f replicas sent a STOP message
			}
			electionResult = lcManager.commitElection(proposedRegencyId);

			LOGGER.info("(Synchronizer.startSynchronization) [{}] -> I am proc {} installing regency {}",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
					lcManager.getNextReg());

			// avoid memory leaks
			lcManager.clearCurrentRequestTimedOut();
			lcManager.clearRequestsFromSTOP();

			// 此时开启太快，会触发反反复复的超时，应该等leaderchange流程结束再开启
//            requestsTimer.Enabled(true);
			requestsTimer.setShortTimeout(-1);
//            requestsTimer.startTimer();

			// int leader = regency % this.reconfManager.getCurrentViewN(); // new leader
			int inExec = tom.getInExec(); // cid to execute
			int lastExec = tom.getLastExec(); // last cid decided

			execManager.setNewLeader(electionResult.getRegency().getLeaderId());

			// 重启心跳
			tom.heartBeatTimer.restart();

			// If I am not the leader, I have to send a STOPDATA message to the elected
			// leader
			if (electionResult.getRegency().getLeaderId() != this.controller.getStaticConf().getProcessId()) {
				sendStopDataInFollower(electionResult, inExec, lastExec);

			} else {
				processStopDataInLeader(electionResult, inExec, lastExec);
			}
		} // End of: if (canSendStopData && lcManager.getNextReg() >
			// lcManager.getLastReg());
	}

	private void processStopDataInLeader(ElectionResult electionResult, int in, int last) {
		final int regency = electionResult.getRegency().getId();
		final int leader = electionResult.getRegency().getLeaderId();

		// If leader, I will store information that I would send in a SYNC message

		LOGGER.info("(Synchronizer.startSynchronization) [{}] -> I am proc {}, I'm the leader for this new regency",
				this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId());
		CertifiedDecision lastDec = null;
		CollectData collect = null;

		Consensus cons = null;

		// Content of the last decided CID
		if (last > -1)
			cons = execManager.getConsensus(last);

		// Do I have info on my last executed consensus?
		if (cons != null && cons.getDecisionEpoch() != null && cons.getDecisionEpoch().propValue != null) {
			// byte[] decision = exec.getLearner().getDecision();

			byte[] decision = cons.getDecisionEpoch().propValue;
			Set<ConsensusMessage> proof = cons.getDecisionEpoch().getProof();

			lastDec = new CertifiedDecision(this.controller.getStaticConf().getProcessId(), last, decision, proof);
			// TODO: WILL BE NECESSARY TO ADD A PROOF!!!??

		} else {
			lastDec = new CertifiedDecision(this.controller.getStaticConf().getProcessId(), last, null, null);

			////// THIS IS TO CATCH A BUG!!!!!
			if (last > -1) {
				LOGGER.error("[DEBUG INFO FOR LAST CID #2]");

				if (cons == null) {
					if (last > -1)
						LOGGER.error("No consensus instance for cid {}", last);

				} else if (cons.getDecisionEpoch() == null) {
					LOGGER.error("No decision epoch for cid {}", last);
				} else {
					LOGGER.error("epoch for cid: {} : {}", last, cons.getDecisionEpoch().toString());

					if (cons.getDecisionEpoch().propValue == null) {
						LOGGER.error("No propose for cid {}", last);
					} else {
						LOGGER.error("Propose hash for cid {} : {}", last,
								Base64.encodeBase64String(tom.computeHash(cons.getDecisionEpoch().propValue)));
					}
				}
				// maybe occur null pointer exception
//                        if (cons.getDecisionEpoch().propValue == null) {
//                            System.out.println("No propose for cid " + last);
//                        } else {
//                            System.out.println("Propose hash for cid " + last + ": " + Base64.encodeBase64String(tom.computeHash(cons.getDecisionEpoch().propValue)));
//                        }
			}

		}
		lcManager.addLastCID(regency, lastDec);

		if (in > -1) { // content of cid being executed
			cons = execManager.getConsensus(in);

			// cons.incEts(); // make the consensus advance to the next epoch
			cons.setETS(regency); // make the consensus advance to the next epoch

			// int ets = cons.getEts();
			// cons.createEpoch(ets, controller);
			cons.createEpoch(regency, controller);
			LOGGER.debug(
					"(Synchronizer.startSynchronization) [{}] -> I am proc {}, in > -1, incrementing ets of consensus {} to {}",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
					cons.getId(), regency);
			TimestampValuePair quorumWrites;

			if (cons.getQuorumWrites() != null) {

				quorumWrites = cons.getQuorumWrites();
			} else {
				quorumWrites = new TimestampValuePair(0, new byte[0]);
			}

			HashSet<TimestampValuePair> writeSet = cons.getWriteSet();

			// collect = new CollectData(this.controller.getStaticConf().getProcessId(), in,
			// ets, quorumWrites, writeSet);
			collect = new CollectData(this.controller.getStaticConf().getProcessId(), in, regency, quorumWrites,
					writeSet);

		} else {

			cons = execManager.getConsensus(last + 1);

			// cons.incEts(); // make the consensus advance to the next epoch
			cons.setETS(regency); // make the consensus advance to the next epoch

			// int ets = cons.getEts();
			// cons.createEpoch(ets, controller);
			cons.createEpoch(regency, controller);
			LOGGER.debug(
					"(Synchronizer.startSynchronization) [{}] -> I am proc {}, in = -1, incrementing ets of consensus {} to {}",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
					cons.getId(), regency);

			// collect = new CollectData(this.controller.getStaticConf().getProcessId(),
			// last + 1, ets, new TimestampValuePair(0, new byte[0]), new
			// HashSet<TimestampValuePair>());
			collect = new CollectData(this.controller.getStaticConf().getProcessId(), last + 1, regency,
					new TimestampValuePair(0, new byte[0]), new HashSet<TimestampValuePair>());
		}

		SignedObject signedCollect = tom.sign(collect);

		lcManager.addCollect(regency, signedCollect);

		// the replica might have received STOPDATAs that were out of context at the
		// time they were received, but now can be processed
		Set<LCMessage> stopdatas = getOutOfContextLC(LCType.STOP_DATA, regency);

//                Logger.println("(Synchronizer.startSynchronization) Checking if there are out of context STOPDATAs for regency " + regency);
		if (stopdatas.size() > 0) {
			LOGGER.info(
					"(Synchronizer.startSynchronization) [{}] -> I am proc {} Processing {} out of context STOPDATAs for regency {}",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
					stopdatas.size(), regency);
		} else {
			LOGGER.info(
					"(Synchronizer.startSynchronization) [{}] -> I am proc {} No out of context STOPDATAs for regency {}",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(), regency);
		}

		for (LCMessage m : stopdatas) {
			processSTOPDATA(m, regency);
		}
	}

	private void sendStopDataInFollower(ElectionResult electionResult, int in, int last) {
		final int regency = electionResult.getRegency().getId();
		final int leader = electionResult.getRegency().getLeaderId();
		ObjectOutputStream out = null;
		ByteArrayOutputStream bos = null;
		try { // serialize content of the STOPDATA message

			bos = new ByteArrayOutputStream();
			out = new ObjectOutputStream(bos);

			Consensus cons = null;

			// content of the last decided CID
			if (last > -1)
				cons = execManager.getConsensus(last);

			// Do I have info on my last executed consensus?
			if (cons != null && cons.getDecisionEpoch() != null && cons.getDecisionEpoch().propValue != null) {

				out.writeBoolean(true);
				out.writeInt(last);
				// byte[] decision = exec.getLearner().getDecision();

				byte[] decision = cons.getDecisionEpoch().propValue;
				Set<ConsensusMessage> proof = cons.getDecisionEpoch().getProof();

				out.writeObject(decision);
				out.writeObject(proof);
				// TODO: WILL BE NECESSARY TO ADD A PROOF!!!

			} else {
				out.writeBoolean(false);

				////// THIS IS TO CATCH A BUG!!!!!
				if (last > -1) {
					LOGGER.error("[DEBUG INFO FOR LAST CID #1]");

					if (cons == null) {
						if (last > -1)
							LOGGER.error("No consensus instance for cid {}", last);

					} else if (cons.getDecisionEpoch() == null) {
						LOGGER.error("No decision epoch for cid {}", last);
					} else {
						LOGGER.error("epoch for cid: {} : {}", last, cons.getDecisionEpoch().toString());

						if (cons.getDecisionEpoch().propValue == null) {
							LOGGER.error("No propose for cid {}", last);
						} else {
							LOGGER.error("Propose hash for cid {} : {}", last,
									Base64.encodeBase64String(tom.computeHash(cons.getDecisionEpoch().propValue)));
						}
					}
				}

			}

			if (in > -1) { // content of cid in execution

				cons = execManager.getConsensus(in);

				// cons.incEts(); // make the consensus advance to the next epoch
				cons.setETS(regency); // make the consensus advance to the next epoch

				// int ets = cons.getEts();
				// cons.createEpoch(ets, controller);
				cons.createEpoch(regency, controller);
				LOGGER.debug(
						"(Synchronizer.startSynchronization) [{}] -> I am proc {} in > -1, incrementing ets of consensus {} to {}",
						this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
						cons.getId(), regency);

				TimestampValuePair quorumWrites;
				if (cons.getQuorumWrites() != null) {

					quorumWrites = cons.getQuorumWrites();

				} else {

					quorumWrites = new TimestampValuePair(0, new byte[0]);
				}

				HashSet<TimestampValuePair> writeSet = cons.getWriteSet();

				// CollectData collect = new
				// CollectData(this.controller.getStaticConf().getProcessId(), in, ets,
				// quorumWrites, writeSet);
				CollectData collect = new CollectData(this.controller.getStaticConf().getProcessId(), in, regency,
						quorumWrites, writeSet);

				SignedObject signedCollect = tom.sign(collect);

				out.writeObject(signedCollect);

			} else {

				cons = execManager.getConsensus(last + 1);

				// cons.incEts(); // make the consensus advance to the next epoch
				cons.setETS(regency); // make the consensus advance to the next epoch

				// int ets = cons.getEts();
				// cons.createEpoch(ets, controller);
				cons.createEpoch(regency, controller);
				// Logger.println("(Synchronizer.startSynchronization) incrementing ets of
				// consensus " + cons.getId() + " to " + ets);
				LOGGER.debug(
						"(Synchronizer.startSynchronization) [{}] -> I am proc {} in = -1, incrementing ets of consensus {} to {}",
						this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
						cons.getId(), regency);

				// CollectData collect = new
				// CollectData(this.controller.getStaticConf().getProcessId(), last + 1, ets,
				// new TimestampValuePair(0, new byte[0]), new HashSet<TimestampValuePair>());
				CollectData collect = new CollectData(this.controller.getStaticConf().getProcessId(), last + 1, regency,
						new TimestampValuePair(0, new byte[0]), new HashSet<TimestampValuePair>());

				SignedObject signedCollect = tom.sign(collect);

				out.writeObject(signedCollect);

			}

			out.flush();
			bos.flush();

			byte[] payload = bos.toByteArray();
			out.close();
			bos.close();

			int[] b = new int[1];
			b[0] = leader;

			LOGGER.info(
					"(Synchronizer.startSynchronization) [{}] -> I am proc {} sending STOPDATA of regency {}, new leader {}, time {}",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(), regency,
					leader, new Date());
			// send message SYNC to the new leader

			int currentLeader = tom.getExecManager().getCurrentLeader();
			LCMessage msgSTOPDATA = LCMessage.createSTOP_DATA(this.controller.getStaticConf().getProcessId(),
					electionResult, payload);
			communication.send(b, msgSTOPDATA);

			// TODO: Turn on timeout again?
		} catch (IOException ex) {
			ex.printStackTrace();
			java.util.logging.Logger.getLogger(TOMLayer.class.getName()).log(Level.SEVERE, null, ex);
		} finally {
			try {
				out.close();
				bos.close();
			} catch (IOException ex) {
				ex.printStackTrace();
				java.util.logging.Logger.getLogger(TOMLayer.class.getName()).log(Level.SEVERE, null, ex);
			}
		}

		// the replica might have received a SYNC that was out of context at the time it
		// was received, but now can be processed
		Set<LCMessage> sync = getOutOfContextLC(LCType.SYNC, regency);

//                Logger.println("(Synchronizer.startSynchronization) Checking if there are out of context SYNC for regency " + regency);

		if (sync.size() > 0) {
			LOGGER.info(
					"(Synchronizer.startSynchronization) [{}] -> I am proc {} Processing out of context SYNC for regency {}",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(), regency);
		} else {
			LOGGER.debug(
					"(Synchronizer.startSynchronization) [{}] -> I am proc {} No out of context SYNC for regency {}",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(), regency);
		}

		for (LCMessage m : sync) {
			if (m.getSender() == execManager.getCurrentLeader()) {
				processSYNC(m.getPayload(), regency);
				return; // makes no sense to continue, since there is only one SYNC message
			}
		}
	}

	/**
	 * 由于收到其它节点报告的 STOP 消息数量满足至少 f+1 个;
	 * 
	 * <br>
	 * 触发当前节点跟随发送 STOP 消息，进入 STOP 状态，开启选举进程；
	 */
	private void broadcast_LC_STOP(LeaderRegencyPropose regencyPropose, boolean triggerBySTOP) {
		final int proposedNewRegency = regencyPropose.getRegency().getId();
		// TODO: 此处有错误！！！！！！ 未延续 STOP 消息的提议 regency；
		if (!lcManager.tryBeginElection(regencyPropose.getRegency())) {
			return;
		}
		heartBeatTimer.stopAll();

		// 当前的领导者；
		final int currentLeader = tom.getExecManager().getCurrentLeader();
		// 加入当前节点的领导者执政期提议；
		// store information about message I am going to send
		lcManager.addStop(regencyPropose);

		// execManager.stop(); // stop execution of consensus

		// Get requests that timed out and the requests received in STOP messages
		// and add those STOPed requests to the client manager
		addSTOPedRequestsToClientManager();
		List<TOMMessage> messages = getRequestsToRelay();

		ObjectOutputStream out = null;
		ByteArrayOutputStream bos = null;
		try { // serialize conent to send in the STOP message
			bos = new ByteArrayOutputStream();
			out = new ObjectOutputStream(bos);

			// Do I have messages to send in the STOP message?
			if (messages != null && messages.size() > 0) {

				// TODO: If this is null, there was no timeout nor STOP messages.
				// What shall be done then?
				out.writeBoolean(true);
				byte[] serialized = bb.makeBatch(messages, 0, 0, controller);
				out.writeObject(serialized);
			} else {
				out.writeBoolean(false);
				LOGGER.debug(
						"(Synchronizer.startSynchronization) [{}] -> Strange... did not include any request in my STOP message for regency {}",
						this.execManager.getTOMLayer().getRealName(), proposedNewRegency);
			}

			out.flush();
			bos.flush();

			byte[] payload = bos.toByteArray();
			out.close();
			bos.close();

			// send message STOP
			LOGGER.info(
					"(Synchronizer.startSynchronization) [{}] -> I am proc {}, sending STOP message to install regency {} with {} request(s) to relay",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
					proposedNewRegency, (messages != null ? messages.size() : 0));

			if (triggerBySTOP) {
				View currentView = this.controller.getCurrentView();
				if (!regencyPropose.isViewEquals(currentView)) {
					throw new IllegalStateException(String.format(
							"The view of regency propose from node[%s] is not equal the the current view of node[%s]!",
							regencyPropose.getSender(), controller.getStaticConf().getProcessId()));
				}

				LCMessage msgSTOP_APPEND = LCMessage.createSTOP_APPEND(this.controller.getStaticConf().getProcessId(),
						regencyPropose.getRegency(), currentView, payload);
				// TODO: ???
				requestsTimer.setSTOP(proposedNewRegency, msgSTOP_APPEND); // make replica re-transmit the stop message
																			// until a
				// new
				// regency is installed
				communication.send(this.controller.getCurrentViewOtherAcceptors(), msgSTOP_APPEND);
			} else {
				LCMessage msgSTOP = LCMessage.createSTOP(this.controller.getStaticConf().getProcessId(),
						regencyPropose.getRegency(), this.controller.getCurrentView(), payload);
				// TODO: ???
				requestsTimer.setSTOP(proposedNewRegency, msgSTOP); // make replica re-transmit the stop message until a
																	// new
				// regency is installed
				communication.send(this.controller.getCurrentViewOtherAcceptors(), msgSTOP);
			}

		} catch (Exception ex) {
			ex.printStackTrace();
			java.util.logging.Logger.getLogger(TOMLayer.class.getName()).log(Level.SEVERE, null, ex);
		} finally {
			try {
				out.close();
				bos.close();
			} catch (IOException ex) {
				ex.printStackTrace();
				java.util.logging.Logger.getLogger(TOMLayer.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
	}

	/**
	 * This method is called by the MessageHandler each time it received messages
	 * related to the leader change
	 *
	 * @param msg Message received from the other replica
	 */
	public void deliverTimeoutRequest(LCMessage msg) {
		switch (msg.getType()) {
		case STOP:
			process_LC_STOP(msg);
			break;
		case STOP_APPEND:
			process_LC_STOP_APPEND(msg);
			break;
		case STOP_DATA:
			process_LC_STOPDATA(msg);
			break;
		case SYNC:
			process_LC_SYNC(msg);
			break;
		default:
			throw new IllegalStateException("Unsupported LCType[" + msg.getType().NAME + "]!");
		}
	}

	private void process_LC_SYNC(LCMessage msg) {
		// message SYNC
		int regency = msg.getReg();

		LOGGER.info(
				"(Synchronizer.deliverTimeoutRequest) [{}] -> I am proc {}, Recv Sync msg, Last regency {}, next regency {}, from proc {}, from port {}",
				this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
				lcManager.getLastReg(), lcManager.getNextReg(), msg.getSender(),
				controller.getStaticConf().getRemoteAddress(msg.getSender()).getConsensusPort());

		// I am expecting this sync?
		boolean isExpectedSync = (regency == lcManager.getLastReg() && regency == lcManager.getNextReg());

		// Is this sync what I wanted to get in the previous iteration of the
		// synchoronization phase?
		boolean islateSync = (regency == lcManager.getLastReg() && regency == (lcManager.getNextReg() - 1));

		// Did I already sent a stopdata in this iteration?
		boolean sentStopdata = (lcManager.getStopsSize(lcManager.getNextReg()) == 0); // if 0, I already purged the
																						// stops,
																						// which I only do when I am
																						// about to
																						// send the stopdata

		// I am (or was) waiting for this message, and did I received it from the new
		// leader?
		if ((isExpectedSync || // Expected case
				(islateSync && !sentStopdata)) && // might happen if I timeout before receiving the SYNC
				(msg.getSender() == execManager.getCurrentLeader())) {

			// if (msg.getReg() == lcManager.getLastReg() &&
			// msg.getReg() == lcManager.getNextReg() && msg.getSender() ==
			// lm.getCurrentLeader()/*(regency % this.reconfManager.getCurrentViewN())*/) {
			processSYNC(msg.getPayload(), regency);

		} else if (msg.getReg() > lcManager.getLastReg()) { // send SYNC to out of context if
			// it is for a future regency
			LOGGER.info(
					"(Synchronizer.deliverTimeoutRequest) [{}] -> I am proc {}, Keeping SYNC message as out of context for regency {}, from proc {}, from port {}",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
					msg.getReg(), msg.getSender(),
					controller.getStaticConf().getRemoteAddress(msg.getSender()).getConsensusPort());
			outOfContextLC.add(msg);

		} else {
			LOGGER.info(
					"(Synchronizer.deliverTimeoutRequest) [{}] -> I am proc {}, Discarding SYNC message, from proc {}, from port {}",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
					msg.getSender(), controller.getStaticConf().getRemoteAddress(msg.getSender()).getConsensusPort());
		}
	}

	private void process_LC_STOPDATA(LCMessage msg) {
		// STOPDATA messages

		int regency = msg.getReg();

		LOGGER.info(
				"(Synchronizer.deliverTimeoutRequest) [{}] -> I am proc {} Recv Stopdata msg, Last regency {}, next regency {}, time {}, from proc {}, from port {}",
				this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
				lcManager.getLastReg(), lcManager.getNextReg(), new Date(), msg.getSender(),
				controller.getStaticConf().getRemoteAddress(msg.getSender()).getConsensusPort());

		// Am I the new leader, and am I expecting this messages?
		if (regency == lcManager.getLastReg() && this.controller.getStaticConf().getProcessId() == execManager
				.getCurrentLeader()/* (regency % this.reconfManager.getCurrentViewN()) */) {

			LOGGER.info(
					"(Synchronizer.deliverTimeoutRequest) [{}] -> I am proc {} I'm the new leader and I received a STOPDATA, from proc {}, from port {}",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
					msg.getSender(), controller.getStaticConf().getRemoteAddress(msg.getSender()).getConsensusPort());
			processSTOPDATA(msg, regency);
		} else if (msg.getReg() > lcManager.getLastReg()) { // send STOPDATA to out of context if
															// it is for a future regency
			LOGGER.info(
					"(Synchronizer.deliverTimeoutRequest) [{}] -> I am proc {} Keeping STOPDATA message as out of context for regency {}, from proc {}, from port {}",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
					msg.getReg(), msg.getSender(),
					controller.getStaticConf().getRemoteAddress(msg.getSender()).getConsensusPort());
			outOfContextLC.add(msg);

		} else {
			LOGGER.error(
					"(Synchronizer.deliverTimeoutRequest) [{}] -> I am proc {} Discarding STOPDATA message, from proc {}, from port {}",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
					msg.getSender(), controller.getStaticConf().getRemoteAddress(msg.getSender()).getConsensusPort());
		}
	}

	private void process_LC_STOP_APPEND(LCMessage msg) {
		//TODO:
		a;
	}

	private void process_LC_STOP(LCMessage msg) {
		// message STOP

		LOGGER.info(
				"(Synchronizer.deliverTimeoutRequest) [{}] -> I am proc {}, Recv Stop msg, Last regency {}, next regency {}, time {}, from proc {}, from port {}",
				this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
				lcManager.getLastReg(), lcManager.getNextReg(), new Date(), msg.getSender(),
				controller.getStaticConf().getRemoteAddress(msg.getSender()).getConsensusPort());

		// TODO: 收到 STOP 消息；
		// this message is for the next leader change?
		LeaderRegencyPropose regencyPropose = LeaderRegencyPropose.copy(msg.getLeader(), msg.getReg(), msg.getViewId(),
				msg.getViewProcessIds(), msg.getSender());
		final int proposedRegencyId = regencyPropose.getRegency().getId();
		if (lcManager.canPropose(proposedRegencyId)) {
			if (lcManager.isFutureRegency(proposedRegencyId)) { // send STOP to out of context
				// 处于选举进程中，但是提议的执政期Id 大于当前选举中的执政期 Id，说明这是一个执政期Id超前的提议；
				// it is for a future regency
				LOGGER.info(
						"(Synchronizer.process_LC_STOP) [{}] -> I am proc {} Keeping STOP message as out of context for regency {}, from proc {}, from port {}",
						this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
						msg.getReg(), msg.getSender(),
						controller.getStaticConf().getRemoteAddress(msg.getSender()).getConsensusPort());
				outOfContextLC.add(msg);
			} else {
				// 未开始选举进程，或者已经开始选举进程并且提议的执政期等于正在选举中的执政期；
				// 等同于表达式：(!lcManager.isInProgress()) || proposedRegencyId ==
				// lcManager.getNextReg()
				LOGGER.info(
						"(Synchronizer.process_LC_STOP) [{}] -> I am proc {} received regency change request, from proc {}, from port {}",
						this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
						msg.getSender(),
						controller.getStaticConf().getRemoteAddress(msg.getSender()).getConsensusPort());

				TOMMessage[] requests = deserializeTOMMessages(msg.getPayload());

				// store requests that came with the STOP message
				lcManager.addRequestsFromSTOP(requests);

				// 当前的领导者；
				// store information about the message STOP

				lcManager.addStop(regencyPropose);

				processOutOfContextSTOPs(msg.getReg()); // the replica might have received STOPs
														// that were out of context at the time they
														// were received, but now can be processed

				startSynchronization(regencyPropose, true); // evaluate STOP messages
			}
		} else {
			LOGGER.error(
					"(Synchronizer.process_LC_STOP) [{}] -> I am proc {} Discarding STOP message, from proc {}, from port {}",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
					msg.getSender(), controller.getStaticConf().getRemoteAddress(msg.getSender()).getConsensusPort());
		}
	}

	// this method is used to verify if the leader can make the message catch-up
	// and also sends the message
	private void catch_up(int regency) {

		LOGGER.debug("(Synchronizer.catch_up) [{}] -> I am proc {} verify STOPDATA info",
				this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId());
		ObjectOutputStream out = null;
		ByteArrayOutputStream bos = null;

		CertifiedDecision lastHighestCID = lcManager.getHighestLastCID(regency);

		int currentCID = lastHighestCID.getCID() + 1;
		HashSet<SignedObject> signedCollects = null;
		byte[] propose = null;
		int batchSize = -1;

		// normalize the collects and apply to them the predicate "sound"
		if (lcManager.sound(lcManager.selectCollects(regency, currentCID))) {

			LOGGER.info("(Synchronizer.catch_up) [{}] -> I am proc {} sound predicate is true",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId());

			signedCollects = lcManager.getCollects(regency); // all original collects that the replica has received

			Decision dec = new Decision(-1); // the only purpose of this object is to obtain the batchsize,
												// using code inside of createPropose()

			propose = tom.createPropose(dec);
			batchSize = dec.batchSize;

			try { // serialization of the CATCH-UP message
				bos = new ByteArrayOutputStream();
				out = new ObjectOutputStream(bos);

				out.writeObject(lastHighestCID);

				// TODO: Missing: serialization of the proof?
				out.writeObject(signedCollects);
				out.writeObject(propose);
				out.writeInt(batchSize);

				out.flush();
				bos.flush();

				byte[] payload = bos.toByteArray();
				out.close();
				bos.close();

				LOGGER.info("(Synchronizer.catch_up) [{}] -> I am proc {}, sending SYNC message for regency {}",
						this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
						regency);

				// send the CATCH-UP message
				int currentLeader = tom.getExecManager().getCurrentLeader();
				LCMessage msgSYNC = LCMessage.createSYNC(this.controller.getStaticConf().getProcessId(), currentLeader,
						regency, payload);
				communication.send(this.controller.getCurrentViewOtherAcceptors(), msgSYNC);

				finalise(regency, lastHighestCID, signedCollects, propose, batchSize, true);

			} catch (IOException ex) {
				ex.printStackTrace();
				java.util.logging.Logger.getLogger(TOMLayer.class.getName()).log(Level.SEVERE, null, ex);
			} finally {
				try {
					out.close();
					bos.close();
				} catch (IOException ex) {
					ex.printStackTrace();
					java.util.logging.Logger.getLogger(TOMLayer.class.getName()).log(Level.SEVERE, null, ex);
				}
			}
		}
	}

	// This method is invoked by the state transfer protocol to notify the replica
	// that it can end synchronization
	public void resumeLC() {

		Consensus cons = execManager.getConsensus(tempLastHighestCID.getCID());
		Epoch e = cons.getLastEpoch();

		int ets = cons.getEts();

		if (e == null || e.getTimestamp() != ets) {
			e = cons.createEpoch(ets, controller);
		} else {
			e.clear();
		}

		byte[] hash = tom.computeHash(tempLastHighestCID.getDecision());
		e.propValueHash = hash;
		e.propValue = tempLastHighestCID.getDecision();

		e.deserializedPropValue = tom.checkProposedValue(tempLastHighestCID.getDecision(), false);

		finalise(tempRegency, tempLastHighestCID, tempSignedCollects, tempPropose, tempBatchSize, tempIAmLeader);

	}

	// this method is called on all replicas, and serves to verify and apply the
	// information sent in the SYNC message
	private void finalise(int regency, CertifiedDecision lastHighestCID, HashSet<SignedObject> signedCollects,
			byte[] propose, int batchSize, boolean iAmLeader) {

		int currentCID = lastHighestCID.getCID() + 1;
		LOGGER.info("(Synchronizer.finalise) [{}] -> I am proc {}, final stage of LC protocol",
				this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId());
		int me = this.controller.getStaticConf().getProcessId();
		Consensus cons = null;
		Epoch e = null;

		if (tom.getLastExec() + 1 < lastHighestCID.getCID()) { // is this a delayed replica?

			LOGGER.info(
					"(Synchronizer.finalise) [{}] -> I am proc {}, NEEDING TO USE STATE TRANSFER!! lastHighest cid {}",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
					lastHighestCID.getCID());

			tempRegency = regency;
			tempLastHighestCID = lastHighestCID;
			tempSignedCollects = signedCollects;
			tempPropose = propose;
			tempBatchSize = batchSize;
			tempIAmLeader = iAmLeader;

			execManager.getStoppedMsgs().add(acceptor.getFactory().createPropose(currentCID, 0, propose));
			stateManager.requestAppState(lastHighestCID.getCID());

			return;

		} /*
			 * else if (tom.getLastExec() + 1 == lastHighestCID.getCID()) { // Is this
			 * replica still executing the last decided consensus?
			 * 
			 * System.out.
			 * println("(Synchronizer.finalise) I'm still at the CID before the most recent one!!! ("
			 * + lastHighestCID.getCID() + ")");
			 * 
			 * cons = execManager.getConsensus(lastHighestCID.getCID()); e =
			 * cons.getLastEpoch();
			 * 
			 * int ets = cons.getEts();
			 * 
			 * if (e == null || e.getTimestamp() != ets) { e = cons.createEpoch(ets,
			 * controller); } else { e.clear(); }
			 * 
			 * byte[] hash = tom.computeHash(lastHighestCID.getCIDDecision());
			 * e.propValueHash = hash; e.propValue = lastHighestCID.getCIDDecision();
			 * 
			 * e.deserializedPropValue =
			 * tom.checkProposedValue(lastHighestCID.getCIDDecision(), false);
			 * cons.decided(e, true); // pass the decision to the delivery thread }
			 */

		// install proof of the last decided consensus
		// 对上个共识的处理开始
		cons = execManager.getConsensus(lastHighestCID.getCID());
		e = null;

		Set<ConsensusMessage> consMsgs = lastHighestCID.getConsMessages();
		if (consMsgs == null)
			consMsgs = new HashSet();

		for (ConsensusMessage cm : consMsgs) {

			if (e == null)
				e = cons.getEpoch(cm.getEpoch(), true, controller);
			if (e.getTimestamp() != cm.getEpoch()) {
				LOGGER.error(
						"(Synchronizer.finalise) [{}] -> I am proc {} Strange... proof of last decided consensus contains messages from more than just one epoch",
						this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId());
				e = cons.getEpoch(cm.getEpoch(), true, controller);
			}
			e.addToProof(cm);

			if (cm.getType() == MessageFactory.ACCEPT) {
				e.setAccept(cm.getSender(), cm.getValue());
			}

			else if (cm.getType() == MessageFactory.WRITE) {
				e.setWrite(cm.getSender(), cm.getValue());
			}

		}
		// 针对上个共识没有完成的进行一些收尾工作
		if (e != null) {

			LOGGER.info("(Synchronizer.finalise) [{}] -> I am proc {}, Installed proof of last decided consensus {}",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
					lastHighestCID.getCID());

			byte[] hash = tom.computeHash(lastHighestCID.getDecision());
			e.propValueHash = hash;
			e.propValue = lastHighestCID.getDecision();
			e.deserializedPropValue = tom.checkProposedValue(lastHighestCID.getDecision(), false);

			// Is this replica still executing the last decided consensus?
			if (tom.getLastExec() + 1 == lastHighestCID.getCID()) {
				LOGGER.info(
						"(Synchronizer.finalise) [{}] -> I am proc {}, I'm still at the CID before the most recent one!!! {}",
						this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
						lastHighestCID.getCID());
				cons.decided(e, true);
			} else {
				// 对于上个共识已经完成的，通过配置false, 控制不再进行写账本的操作
				cons.decided(e, false);
			}

		} else {
			LOGGER.info(
					"(Synchronizer.finalise) [{}] -> I am proc {}, I did not install any proof of last decided consensus {}",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
					lastHighestCID.getCID());
		}
		// 对上个共识的处理结束
		cons = null;
		e = null;

		// 开启对大家当前共识的处理
		// get a value that satisfies the predicate "bind"
		byte[] tmpval = null;
		HashSet<CollectData> selectedColls = lcManager.selectCollects(signedCollects, currentCID, regency);

		// getBindValue的目的就是找一个处于当前共识currentCID中的序列化后的提议值，如果currentCID中所有节点都没有收到过propose,
		// write，则会返回空，tmpval为空，则会使用从客户端请求队列中创建的新propose, 也是finalise传进来的参数
		tmpval = lcManager.getBindValue(selectedColls);
		LOGGER.debug("(Synchronizer.finalise) [{}] -> I am proc {}, Trying to find a binded value",
				this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId());

		// If such value does not exist, obtain the value written by the arguments
		if (tmpval == null && lcManager.unbound(selectedColls) && batchSize > 0) {
			LOGGER.info(
					"(Synchronizer.finalise) [{}] -> I am proc {}, did not found a value that might have already been decided, so use new propose!",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId());
			tmpval = propose;
		} else if (tmpval == null && batchSize == 0) {
			LOGGER.info(
					"(Synchronizer.finalise) [{}] -> I am proc {} not found a value that might have been decided,and propose is null",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId());
		} else {
			LOGGER.info("(Synchronizer.finalise) [{}] -> I am proc {} found a value that might have been decided",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId());
		}

		if (tmpval != null) { // did I manage to get some value?

			LOGGER.info("(Synchronizer.finalise) [{}] -> I am proc {} resuming normal phase",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId());
			lcManager.removeCollects(regency); // avoid memory leaks

			// stop the re-transmission of the STOP message for all regencies up to this one
			removeSTOPretransmissions(regency);

			cons = execManager.getConsensus(currentCID);

			e = cons.getLastEpoch();

			int ets = cons.getEts();

			// Update current consensus with latest ETS. This may be necessary
			// if I 'jumped' to a consensus instance ahead of the one I was executing

			// int currentETS = lcManager.getETS(currentCID, selectedColls);
			// if (currentETS > ets) {
			if (regency > ets) {

				// System.out.println("(Synchronizer.finalise) Updating consensus' ETS after
				// SYNC (from " + ets + " to " + currentETS +")");
				LOGGER.info(
						"(Synchronizer.finalise) [{}] -> I am proc {} Updating consensus' ETS after SYNC (from {} to {}",
						this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(), ets,
						regency);

				/*
				 * do { cons.incEts(); } while (cons.getEts() != currentETS);
				 */

				// 对于在领导者切换过程中已经收到三个Write，即已经进行了预计算的节点，需要进行预计算的回滚，因为领导者切换完成时会对本轮共识重新发送write消息，重新执行共识过程；
				if (cons != null && cons.getPrecomputed() && !cons.getPrecomputeCommited()) {

					DefaultRecoverable defaultRecoverable = ((DefaultRecoverable) tom.getDeliveryThread().getReceiver()
							.getExecutor());
					if (e != null) {
						defaultRecoverable.preComputeRollback(cons.getId(), e.getBatchId());
					}
				}
				cons.setETS(regency);

				// cons.createEpoch(currentETS, controller);
				cons.createEpoch(regency, controller);

				e = cons.getLastEpoch();
			}

			// Make sure the epoch is created
			/*
			 * if (e == null || e.getTimestamp() != ets) { e = cons.createEpoch(ets,
			 * controller); } else { e.clear(); }
			 */
			if (e == null || e.getTimestamp() != regency) {
				e = cons.createEpoch(regency, controller);
			} else {
				e.clear();
			}

			/********* LEADER CHANGE CODE ********/
			cons.removeWritten(tmpval);
			cons.addWritten(tmpval);
			/*************************************/

			byte[] hash = tom.computeHash(tmpval);
			e.propValueHash = hash;
			e.propValue = tmpval;

			e.deserializedPropValue = tom.checkProposedValue(tmpval, false);

			if (e.deserializedPropValue != null && e.deserializedPropValue.length > 0) {
				e.setProposeTimestamp(e.deserializedPropValue[0].timestamp);
			}

			if (cons.getDecision().firstMessageProposed == null) {
				if (e.deserializedPropValue != null && e.deserializedPropValue.length > 0) {
					cons.getDecision().firstMessageProposed = e.deserializedPropValue[0];
				} else {
					cons.getDecision().firstMessageProposed = new TOMMessage(); // to avoid null pointer
				}
			}
			if (this.controller.getStaticConf().isBFT()) {
				e.setWrite(me, hash);
			} else {
				e.setAccept(me, hash);

				/********* LEADER CHANGE CODE ********/
				LOGGER.debug(
						"(Synchronizer.finalise) [CFT Mode] Setting consensus {} QuorumWrite tiemstamp to {} and value {}",
						currentCID, e.getConsensus().getEts(), Arrays.toString(hash));
				e.getConsensus().setQuorumWrites(hash);
				/*************************************/

			}

			// resume normal operation
			execManager.restart();
			// leaderChanged = true;
			tom.setInExec(currentCID);

//            tom.heartBeatTimer.stopLeaderChange();

			if (iAmLeader) {
				LOGGER.info("(Synchronizer.finalise) [{}] -> I am proc {} wake up proposer thread",
						this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId());
				tom.imAmTheLeader();
			} // waik up the thread that propose values in normal operation

			// send a WRITE/ACCEPT message to the other replicas
			if (this.controller.getStaticConf().isBFT()) {
				LOGGER.info(
						"(Synchronizer.finalise) [{}] -> I am proc {} sending WRITE message for CID {}, timestamp {}, value {}",
						this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(),
						currentCID, e.getTimestamp(), Arrays.toString(e.propValueHash));
				// 有了propose值，各个节点从发送write消息开始，重新进行共识流程；
				communication.send(this.controller.getCurrentViewOtherAcceptors(),
						acceptor.getFactory().createWrite(currentCID, e.getTimestamp(), e.propValueHash));
			} else {
				LOGGER.info("(Synchronizer.finalise) sending ACCEPT message for CID {}, timestamp {}, value {}",
						currentCID, e.getTimestamp(), Arrays.toString(e.propValueHash));
				communication.send(this.controller.getCurrentViewOtherAcceptors(),
						acceptor.getFactory().createAccept(currentCID, e.getTimestamp(), e.propValueHash));
			}
			// all peers' inexecid is -1, and peer's pending request queue is null
		} else if (batchSize == 0) {
			LOGGER.info("(Synchronizer.finalise) [{}] -> I am proc {} batch size is 0",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId());

			lcManager.removeCollects(regency); // avoid memory leaks

			// stop the re-transmission of the STOP message for all regencies up to this one
			removeSTOPretransmissions(regency);

//            tom.heartBeatTimer.stopLeaderChange();

			// resume normal operation
			execManager.restart();
			// leaderChanged = true;

			if (iAmLeader) {
				LOGGER.info("(Synchronizer.finalise) [{}] -> I am proc {} wake up proposer thread",
						this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId());
				tom.imAmTheLeader();
			} // waik up the thread that propose values in normal operation

			execManager.removeSingleConsensus(currentCID);

		} else {
			LOGGER.info("(Synchronizer.finalise) [{}] -> I am proc {}, sync phase failed for regency {}",
					this.execManager.getTOMLayer().getRealName(), controller.getStaticConf().getProcessId(), regency);
		}
	}

}
