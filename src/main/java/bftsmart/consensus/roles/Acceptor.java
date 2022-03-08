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
package bftsmart.consensus.roles;

import bftsmart.communication.MacKey;
import bftsmart.communication.ServerCommunicationSystem;
import bftsmart.consensus.Consensus;
import bftsmart.consensus.Epoch;
import bftsmart.consensus.app.BatchAppResult;
import bftsmart.consensus.app.ComputeCode;
import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.consensus.messages.MessageFactory;
import bftsmart.reconfiguration.ReconfigureRequest;
import bftsmart.reconfiguration.ReplicaTopology;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.reconfiguration.views.NodeNetwork;
import bftsmart.reconfiguration.views.View;
import bftsmart.tom.core.ExecutionManager;
import bftsmart.tom.core.ReplyManager;
import bftsmart.tom.core.TOMLayer;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.server.Replier;
import bftsmart.tom.server.defaultservices.DefaultRecoverable;
import bftsmart.tom.util.TOMUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class represents the acceptor role in the consensus protocol. This class
 * work together with the TOMLayer class in order to supply a atomic multicast
 * service.
 *
 * @author Alysson Bessani
 */
public final class Acceptor {
	private static final Logger LOGGER = LoggerFactory.getLogger(Acceptor.class);

	// 最大尝试次数
	private static final int MAX_RETRY_SIZE = 3;

	private int me; // This replica ID
	private ExecutionManager executionManager; // Execution manager of consensus's executions
	private MessageFactory factory; // Factory for PaW messages
	private ServerCommunicationSystem communication; // Replicas comunication system
	private TOMLayer tomLayer; // TOM layer
	private ReplicaTopology topology;
	// private Cipher cipher;
//	private Mac mac;

	private LinkedBlockingQueue<ConsensusMessage> consensusMessageQueue = new LinkedBlockingQueue<>();
	private volatile boolean doWork = false;
//	private volatile Thread thrdWork;

	/**
	 * Creates a new instance of Acceptor.
	 * 
	 * @param communication Replicas communication system
	 * @param factory       Message factory for PaW messages
	 * @param topology
	 */
	public Acceptor(ServerCommunicationSystem communication, MessageFactory factory, ServerViewController topology) {
		this.communication = communication;
		this.me = topology.getStaticConf().getProcessId();
		this.factory = factory;
		this.topology = topology;
//		try {
//			// this.cipher = Cipher.getInstance("DES/ECB/PKCS5Padding");
//			// this.cipher = Cipher.getInstance(ServerConnection.MAC_ALGORITHM);
//			this.mac = Mac.getInstance(MessageConnection.MAC_ALGORITHM);
//		} catch (NoSuchAlgorithmException /* | NoSuchPaddingException */ ex) {
//			ex.printStackTrace();
//		}
	}

	public DefaultRecoverable getDefaultExecutor() {
		return (DefaultRecoverable) tomLayer.getDeliveryThread().getReceiver().getExecutor();
	}

	public Replier getBatchReplier() {
		return tomLayer.getDeliveryThread().getReceiver().getReplier();
	}

	public ReplyManager getReplyManager() {
		return tomLayer.getDeliveryThread().getReceiver().getRepMan();
	}

	public MessageFactory getFactory() {
		return factory;
	}

	/**
	 * Sets the execution manager for this acceptor
	 * 
	 * @param manager Execution manager for this acceptor
	 */
	public void setExecutionManager(ExecutionManager manager) {
		this.executionManager = manager;
	}

	/**
	 * Sets the TOM layer for this acceptor
	 * 
	 * @param tom TOM layer for this acceptor
	 */
	public void setTOMLayer(TOMLayer tom) {
		this.tomLayer = tom;
	}

	/**
	 * Called by communication layer to delivery Paxos messages. This method only
	 * verifies if the message can be executed and calls process message (storing it
	 * on an out of context message buffer if this is not the case)
	 *
	 * @param msg Paxos messages delivered by the communication layer
	 */
	public final void deliver(ConsensusMessage msg) {
		if (executionManager.checkLimits(msg)) {
//            LOGGER.debug("processing paxos msg with id " + msg.getNumber());
			LOGGER.debug("processing paxos msg with id {}", msg.getNumber());
			processMessage(msg);
		} else {
//            LOGGER.debug("out of context msg with id " + msg.getNumber());
			LOGGER.debug("out of context msg with id {}", msg.getNumber());

			tomLayer.processOutOfContext();
			tomLayer.processOutOfContextWriteAndAccept();
		}
	}

	public boolean checkSucc(Consensus consensus, int msgEpoch) {

		Epoch latestEpochObj = consensus.getLastEpoch();

		if (latestEpochObj == null) {
			return true;
		}

		int latestEpoch = latestEpochObj.getTimestamp();

		// 说明发生过领导者切换，本节点参与了领导者切换流程，并更新了本共识的时间戳，此时又收到老时间戳内的共识消息，对于这种共识消息不再处理
		if (msgEpoch < latestEpoch) {
			LOGGER.info("I am proc {}, checkSucc false, msgEpoch = {}, latestEpoch = {}", topology.getStaticConf().getProcessId(), msgEpoch, latestEpoch);
			return false;
		}

		// 说明本节点因为网络原因没有参与到领导者切换的流程；网络恢复后，收到时间戳前进后的共识消息；本分支让没有参与到领导者切换过程的节点后续走状态传输的过程去进行本地的更新；
		if ((tomLayer.getInExec() == consensus.getId()) && (msgEpoch > latestEpoch)) {
			LOGGER.info("I am proc {}, checkSucc false, msgEpoch = {}, latestEpoch = {}", topology.getStaticConf().getProcessId(), msgEpoch, latestEpoch);
			// 如果本轮共识已经处理完成并且提交了，不再处理该消息；
			// 如果没有提交，但是已经进行了预计算，需要对预计算进行回滚；
			// 如果本轮共识没有走到预计算的过程，对于新时间戳内的共识消息也不做处理
			// 本过程就是让没有参与到领导者切换过程的节点
			if (consensus.getPrecomputed() && !consensus.getPrecomputeCommited()) {
				Epoch epoch = consensus.getEpoch(latestEpoch, topology);
				consensus.lock.lock();
				try {
					getDefaultExecutor().preComputeRollback(consensus.getId(), epoch.getBatchId());
				} finally {
					consensus.lock.unlock();
				}
			}
			return false;
		}

		return true;
	}

	/**
	 * Called when a Consensus message is received or when a out of context message
	 * must be processed. It processes the received message according to its type
	 *
	 * @param msg The message to be processed
	 */
	public final void processMessage(ConsensusMessage msg) {
		Consensus consensus = executionManager.getConsensus(msg.getNumber());

		// 该版本添加特殊处理，后续需要考虑优化掉该处理
//		if (msg.getType() != MessageFactory.PROPOSE && consensus.getLastEpoch() != null && consensus.getLastEpoch().getTimestamp() > msg.getEpoch()) {
//			msg = new ConsensusMessage(msg.getType(),msg.getNumber(),consensus.getLastEpoch().getTimestamp(), msg.getSender(), msg.getValue());
//		}

		// 检查消息的epoch
		if (!checkSucc(consensus, msg.getEpoch())) {
			LOGGER.info("I am proc {}, msg type = {}, processMessage checkSucc failed!", topology.getStaticConf().getProcessId(), msg.getType());
			return;
		}


		// 收到的共识消息对应的时间戳
		Epoch poch = consensus.getEpoch(msg.getEpoch(), topology);

		switch (msg.getType()) {
		case MessageFactory.PROPOSE: {
			while (doWork && (!tomLayer.isReady())) {
				LOGGER.warn("Wait for the node[{}] to be ready... ", topology.getCurrentProcessId());
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
				}
			}

			consensus.lock.lock();
			try {
				proposeReceived(poch, msg);
			} finally {
				consensus.lock.unlock();
			}
		}
			break;
		case MessageFactory.WRITE: {
			consensus.lock.lock();
			try {
				writeReceived(poch, msg.getSender(), msg.getValue());
			} finally {
				consensus.lock.unlock();
			}
		}
			break;
		case MessageFactory.ACCEPT: {
			consensus.lock.lock();
			try {
				acceptReceived(poch, msg);
			} finally {
				consensus.lock.unlock();
			}
		}
		}// End of : switch (msg.getType());
	}

	/**
	 * Called when a PROPOSE message is received or when processing a formerly out
	 * of context propose which is know belongs to the current consensus.
	 *
	 * @param msg The PROPOSE message to by processed
	 */
	private void proposeReceived(Epoch epoch, ConsensusMessage msg) {
		int cid = epoch.getConsensus().getId();
		int ts = epoch.getConsensus().getEts();
		int ets = executionManager.getConsensus(msg.getNumber()).getEts();
//    	LOGGER.debug("(Acceptor.proposeReceived) PROPOSE for consensus " + cid);

		LOGGER.debug("(Acceptor.proposeReceived) I am proc {}, PROPOSE for consensus {} ",
				topology.getStaticConf().getProcessId(), cid);

		if (msg.getSender() == executionManager.getCurrentLeader() // Is the replica the leader?
				&& epoch.getTimestamp() == 0 && ts == ets && ets == 0) { // Is all this in epoch 0?
			executePropose(epoch, msg.getValue());
		} else {
			LOGGER.error("Propose received is not from the expected leader");
		}
	}

//	private boolean isReady() {
//		if (tomLayer == null || tomLayer.getStateManager().isRetrievingState()) {
//			return false;
//		}
//
//		if (tomLayer == null || (!tomLayer.heartBeatTimer.isActived())) {
//			return false;
//		}
//		return true;
//	}

	/**
	 * Executes actions related to a proposed value.
	 *
	 * @param epoch the current epoch of the consensus
	 * @param value Value that is proposed
	 */
	private void executePropose(Epoch epoch, byte[] value) {

		try {
			int cid = epoch.getConsensus().getId();
			LOGGER.info("(Acceptor.executePropose) I am proc {}, executing propose for cid : {}, epoch timestamp: {}",
					topology.getStaticConf().getProcessId(), cid, epoch.getTimestamp());

			long consensusStartTime = System.nanoTime();

			if (epoch.propValue == null) { // only accept one propose per epoch
				epoch.propValue = value;
				epoch.propValueHash = tomLayer.computeHash(value);

				/*** LEADER CHANGE CODE ********/
				epoch.getConsensus().addWritten(value);
				LOGGER.debug(
						"(Acceptor.executePropose) I have written value {}, in consensus instance {}, with timestamp {}",
						Arrays.toString(epoch.propValueHash), cid, epoch.getConsensus().getEts());
				/*****************************************/

				// start this consensus if it is not already running
				if (cid == tomLayer.getLastExec() + 1) {
					tomLayer.setInExec(cid);
				}
				epoch.deserializedPropValue = tomLayer.checkProposedValue(value, true);
				if (epoch.deserializedPropValue != null && epoch.deserializedPropValue.length > 0) {
					epoch.setProposeTimestamp(epoch.deserializedPropValue[0].timestamp);
				}

				if (epoch.deserializedPropValue != null && !epoch.isWriteSetted(me)) {
					if (epoch.getConsensus().getDecision().firstMessageProposed == null) {
						epoch.getConsensus().getDecision().firstMessageProposed = epoch.deserializedPropValue[0];
					}
					if (epoch.getConsensus().getDecision().firstMessageProposed.consensusStartTime == 0) {
						epoch.getConsensus().getDecision().firstMessageProposed.consensusStartTime = consensusStartTime;

					}
					epoch.getConsensus().getDecision().firstMessageProposed.proposeReceivedTime = System.nanoTime();

					if (topology.getStaticConf().isBFT()) {
						LOGGER.debug("(Acceptor.executePropose) sending WRITE for {}", cid);

						epoch.setWrite(me, epoch.propValueHash);
						epoch.getConsensus().getDecision().firstMessageProposed.writeSentTime = System.nanoTime();

//                    System.out.println("I am proc " + controller.getStaticConf().getProcessId() + ", send write msg" + ", cid is " + cid);
						communication.send(this.topology.getCurrentViewOtherAcceptors(),
								factory.createWrite(cid, epoch.getTimestamp(), epoch.propValueHash));

						LOGGER.debug("(Acceptor.executePropose) WRITE sent for {}", cid);

						computeWrite(cid, epoch, epoch.propValueHash);

						LOGGER.debug("(Acceptor.executePropose) WRITE computed for {}", cid);

					} else {
						epoch.setAccept(me, epoch.propValueHash);
						epoch.getConsensus().getDecision().firstMessageProposed.writeSentTime = System.nanoTime();
						epoch.getConsensus().getDecision().firstMessageProposed.acceptSentTime = System.nanoTime();
						/**** LEADER CHANGE CODE! ******/
						LOGGER.debug(
								"(Acceptor.executePropose) [CFT Mode] Setting consensus {}, QuorumWrite tiemstamp to {} and value {}",
								cid, epoch.getConsensus().getEts(), Arrays.toString(epoch.propValueHash));
						epoch.getConsensus().setQuorumWrites(epoch.propValueHash);
						/*****************************************/

						communication.send(this.topology.getCurrentViewOtherAcceptors(),
								factory.createAccept(cid, epoch.getTimestamp(), epoch.propValueHash));

						computeAccept(cid, epoch, epoch.propValueHash);
					}
					executionManager.processOutOfContext(epoch.getConsensus());
				}
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}

	}

	/**
	 * Called when a WRITE message is received
	 *
	 * @param epoch Epoch of the receives message
	 * @param a     Replica that sent the message
	 * @param value Value sent in the message
	 */
	private void writeReceived(Epoch epoch, int a, byte[] value) {
		int cid = epoch.getConsensus().getId();
		LOGGER.debug("(Acceptor.writeAcceptReceived) WRITE from {} for consensus {}", a, cid);
		epoch.setWrite(a, value);

		computeWrite(cid, epoch, value);
	}

	/**
	 * merge byte array
	 * 
	 * @param prop    serialized prop value
	 * @param appHash app hash vaule
	 * @return
	 */
	public byte[] MergeByte(byte[] prop, byte[] appHash) {
		byte[] result = new byte[prop.length + appHash.length];
		System.arraycopy(prop, 0, result, 0, prop.length);
		System.arraycopy(appHash, 0, result, prop.length, appHash.length);
		return result;
	}

	/**
	 * Computes WRITE values according to Byzantine consensus specification values
	 * received).
	 *
	 * @param cid   Consensus ID of the received message
	 * @param epoch Epoch of the receives message
	 * @param value Value sent in the message
	 */
	private void computeWrite(int cid, Epoch epoch, byte[] value) {
		try {
			int writeAccepted = epoch.countWrite(value);

			if (writeAccepted > topology.getQuorum()) {
				LOGGER.info("(Acceptor.computeWrite) I am proc {}, I have {} WRITEs for cid {}, epoch timestamp {}",
						this.topology.getStaticConf().getProcessId(), writeAccepted, cid, epoch.getTimestamp());

//            System.out.println("(computeWrite) I am proc " + controller.getStaticConf().getProcessId() + ", my propose value hash is " + epoch.propValueHash + ", recv propose hash is "+ value + ", cid is " + cid + ", epoch is " + epoch.getTimestamp());

				if (!epoch.isAcceptSetted(me) && Arrays.equals(value, epoch.propValueHash)) {

					LOGGER.debug("(Acceptor.computeWrite) I am proc {} sending WRITE for {}",
							this.topology.getStaticConf().getProcessId(), cid);

					/**** LEADER CHANGE CODE! ******/
					LOGGER.debug(
							"(Acceptor.computeWrite) Setting consensus {} , QuorumWrite tiemstamp to {} and value {}",
							cid, epoch.getConsensus().getEts(), Arrays.toString(value));
					epoch.getConsensus().setQuorumWrites(value);
					/*****************************************/

					if (epoch.getConsensus().getDecision().firstMessageProposed != null) {

						epoch.getConsensus().getDecision().firstMessageProposed.acceptSentTime = System.nanoTime();
					}

					// add to implement application consistency
					if (topology.getStaticConf().isBFT()) {

						DefaultRecoverable defaultExecutor = getDefaultExecutor();
                        byte[][] commands = new byte[epoch.deserializedPropValue.length][];
//						List<byte[]> commands = new ArrayList<byte[]>();

						for (int i = 0; i < epoch.deserializedPropValue.length; i++) {
							// 对于视图ID落后于当前节点视图ID的请求或者Reconfig请求不进行预计算处理
//							if (ViewIdBackWard(epoch.deserializedPropValue[i])
//									|| isReconfig(epoch.deserializedPropValue[i])) {
//								continue;
//							}

							TOMMessage message = epoch.deserializedPropValue[i];
							if (isReconfig(message)) {
								ReconfigureRequest request = (ReconfigureRequest) TOMUtil.getObject(message.getContent());
								commands[i] = request.getExtendInfo();
							} else {
								commands[i] = message.getContent();
							}

//							epoch.deserializedPrecomputeValue.add(msg);

						}

						LOGGER.info("I am proc {}, start pre compute , cid = {}, epoch = {}", this.topology.getStaticConf().getProcessId(), cid, epoch.getTimestamp());
						BatchAppResult appHashResult = defaultExecutor.preComputeHash(cid, commands, epoch.getProposeTimestamp());

						byte[] result = MergeByte(epoch.propValue, appHashResult.getAppHashBytes());
						epoch.propAndAppValue = result;

						epoch.propAndAppValueHash = tomLayer.computeHash(result);

						epoch.preComputeRes = appHashResult.getComputeCode();

						epoch.commonHash = appHashResult.getGenisHashBytes();

						tomLayer.getExecManager().getConsensus(cid).setPrecomputed(true);

						epoch.setAsyncResponseLinkedList(appHashResult.getAsyncResponses());

						epoch.batchId = appHashResult.getBatchId();

						epoch.setAccept(me, epoch.propAndAppValueHash);

						ConsensusMessage cm = factory.createAccept(cid, epoch.getTimestamp(),
								epoch.propAndAppValueHash);

						// add origin propose hash for accept type consensus msg
						cm.setOrigPropValue(epoch.propValueHash);

						// Create a cryptographic proof for this ACCEPT message
						LOGGER.debug(
								"(Acceptor.computeWrite) Creating cryptographic proof for my ACCEPT message from consensus {}",
								cid);
						insertProof(cm, epoch);

						int[] targets = this.topology.getCurrentViewOtherAcceptors();
//                    System.out.println("I am proc " + controller.getStaticConf().getProcessId() + ", send accept msg" + ", cid is "+cid);
						communication.send(targets, cm);
//                    communication.getServersConn().send(targets, cm, true);

						epoch.addToProof(cm);
						computeAccept(cid, epoch, epoch.propAndAppValueHash);
					} else {
						epoch.setAccept(me, value);

						ConsensusMessage cm = factory.createAccept(cid, epoch.getTimestamp(), value);

						// Create a cryptographic proof for this ACCEPT message
						LOGGER.debug(
								"(Acceptor.computeWrite) Creating cryptographic proof for my ACCEPT message from consensus {}",
								cid);
						insertProof(cm, epoch);

						int[] targets = this.topology.getCurrentViewOtherAcceptors();
						communication.getServersCommunication().send(targets, cm, true);

						// communication.send(this.reconfManager.getCurrentViewOtherAcceptors(),
						// factory.createStrong(cid, epoch.getNumber(), value));
						epoch.addToProof(cm);
						computeAccept(cid, epoch, value);

					}
				}
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	/**
	 * Create a cryptographic proof for a consensus message
	 * 
	 * This method modifies the consensus message passed as an argument, so that it
	 * contains a cryptographic proof.
	 * 
	 * @param cm    The consensus message to which the proof shall be set
	 * @param epoch The epoch during in which the consensus message was created
	 */
	private void insertProof(ConsensusMessage cm, Epoch epoch) {
		ByteArrayOutputStream bOut = new ByteArrayOutputStream(248);
		try {
			new ObjectOutputStream(bOut).writeObject(cm);
		} catch (IOException ex) {
			ex.printStackTrace();
		}

		byte[] data = bOut.toByteArray();

		// check if consensus contains reconfiguration request
		TOMMessage[] msgs = epoch.deserializedPropValue;
		boolean hasReconf = false;

		for (TOMMessage msg : msgs) {
			if (msg.getReqType() == TOMMessageType.RECONFIG && msg.getViewID() == topology.getCurrentViewId()) {
				hasReconf = true;
				break; // no need to continue, exit the loop
			}
		}

		// If this consensus contains a reconfiguration request, we need to use
		// signatures (there might be replicas that will not be part of the next
		// consensus instance, and so their MAC will be outdated and useless)
		if (hasReconf) {

			PrivateKey RSAprivKey = topology.getStaticConf().getRSAPrivateKey();

			byte[] signature = TOMUtil.signMessage(RSAprivKey, data);

			cm.setProof(signature);

		} else { // ... if not, we can use MAC vectores
			int[] processes = this.topology.getCurrentViewProcesses();

			HashMap<Integer, byte[]> macVector = new HashMap<>();

			for (int id : processes) {
				if (id == me) {
					continue;
				}
				int retrySize = 0;
				try {
//					SecretKey key = null;
					MacKey macKey = null;
					while (retrySize < MAX_RETRY_SIZE) {

//						key = communication.getServersCommunication().getSecretKey(id);
						macKey = tomLayer.getCommunication().getServersCommunication()
								.getMacKey(id);
						if (macKey == null) {
							LOGGER.error("(Acceptor.insertProof) I don't have yet a secret key with {} . Retrying.",
									id);
							retrySize++;
							Thread.sleep(10);
						} else {
							break;
						}
					}
					if (macKey != null) {
//						this.mac.init(key);
						byte[] macBytes = macKey.generateMac(data);
						macVector.put(id, macBytes);
					}
//
//                    do {
//                        key = communication.getServersConn().getSecretKey(id);
//                        if (key == null) {
//                            LOGGER.error("(Acceptor.insertProof) I don't have yet a secret key with {} . Retrying.", id);
//                            Thread.sleep(1000);
//                        }
//
//                    } while (key == null);  // JCS: This loop is to solve a race condition where a
//                                            // replica might have already been insert in the view or
//                                            // recovered after a crash, but it still did not concluded
//                                            // the diffie helman protocol. Not an elegant solution,
//                                            // but for now it will do
//                    this.mac.init(key);
//                    macVector.put(id, this.mac.doFinal(data));
				} catch (InterruptedException ex) {
					ex.printStackTrace();
				}
			}

			cm.setProof(macVector);
		}

	}

	/**
	 * Called when a ACCEPT message is received
	 * 
	 * @param epoch Epoch of the receives message
	 * @param a     Replica that sent the message
	 * @param value Value sent in the message
	 */
	private void acceptReceived(Epoch epoch, ConsensusMessage msg) {
		int cid = epoch.getConsensus().getId();
		LOGGER.debug("(Acceptor.acceptReceived) ACCEPT from {} for consensus {}", msg.getSender(), cid);
		epoch.setAccept(msg.getSender(), msg.getValue());
		epoch.addToProof(msg);

		computeAccept(cid, epoch, msg.getValue());
	}

//	private void updateConsensusSetting(Epoch epoch) {

//		TOMMessage[] requests = epoch.deserializedPropValue;
//
//		if (requests != null) {
//			tomLayer.clientsManager.requestsPending(requests);
//		}

//		tomLayer.setLastExec(tomLayer.getInExec());
//
//		tomLayer.setInExec(-1);

//	}

//    private void createResponses(Epoch epoch,  List<byte[]> updatedResp) {
//
//        TOMMessage[] requests = epoch.deserializedPrecomputeValue.toArray(new TOMMessage[epoch.deserializedPrecomputeValue.size()]);
//
//        Replier replier = getBatchReplier();
//
//        ReplyManager repMan = getReplyManager();
//
//        for (int index = 0; index < requests.length; index++) {
//            TOMMessage request = requests[index];
//            request.reply = new TOMMessage(me, request.getSession(), request.getSequence(),
//                    request.getOperationId(), updatedResp.get(index), controller.getCurrentViewId(),
//                    request.getReqType());
//
//            if (controller.getStaticConf().getNumRepliers() > 0) {
//                LOGGER.debug("(ServiceReplica.receiveMessages) sending reply to {} with sequence number {} and and operation ID {} via ReplyManager", request.getSender(), request.getSequence()
//                        , request.getOperationId());
//                repMan.send(request);
//            } else {
//                LOGGER.debug("(ServiceReplica.receiveMessages) sending reply to {} with sequence number {} and operation ID {}"
//                        , request.getSender(), request.getSequence(), request.getOperationId());
//                replier.manageReply(request, null);
//                // cs.send(new int[]{request.getSender()}, request.reply);
//            }
//        }
//
//    }
	/**
	 * Computes ACCEPT values according to the Byzantine consensus specification
	 * 
	 * @param epoch Epoch of the receives message
	 * @param value Value sent in the message
	 */
	private void computeAccept(int cid, Epoch epoch, byte[] value) {
		try {
			List<byte[]> updatedResp;

			if (epoch.countAccept(value) > topology.getQuorum() && !epoch.getConsensus().isDecided()) {
				LOGGER.info("(Acceptor.computeAccept) I am proc {}, I have {} ACCEPTs for cid {} and timestamp {}",
						topology.getStaticConf().getProcessId(), epoch.countAccept(value), cid, epoch.getTimestamp());
				if (Arrays.equals(value, epoch.propAndAppValueHash)
						&& (ComputeCode.valueOf(epoch.getPreComputeRes()) == ComputeCode.SUCCESS)) {
					LOGGER.debug("(Acceptor.computeAccept) I am proc {}. Deciding {} ",
							topology.getStaticConf().getProcessId(), cid);
					try {
						LOGGER.info("(Acceptor.computeAccept) I am proc {}, I will write cid {} 's propse to ledger",
								topology.getStaticConf().getProcessId(), cid);
						// 发生过预计算才会进行commit的操作,对于视图ID号小的请求以及视图更新的重配请求没有进行过预计算，不需要提交
						getDefaultExecutor().preComputeCommit(cid, epoch.getBatchId());
						tomLayer.getExecManager().getConsensus(cid).setPrecomputeCommited(true);
						decide(epoch);
					} catch (Exception e) {
						// maybe storage exception
						LOGGER.error("I am proc {} , flush storage fail, will rollback!",
								topology.getStaticConf().getProcessId());
						getDefaultExecutor().preComputeRollback(cid, epoch.getBatchId());
//						updateConsensusSetting(epoch);
						updatedResp = getDefaultExecutor().updateResponses(epoch.getAsyncResponseLinkedList(),
								epoch.commonHash, true);
						epoch.setAsyncResponseLinkedList(updatedResp);
						epoch.setRollback(true);
						decide(epoch);
					}
				} else if (Arrays.equals(value, epoch.propAndAppValueHash)
						&& (ComputeCode.valueOf(epoch.getPreComputeRes()) == ComputeCode.FAILURE)) {
					LOGGER.error("I am proc {} , cid {}, precompute fail, will rollback",
							topology.getStaticConf().getProcessId(), cid);
					getDefaultExecutor().preComputeRollback(cid, epoch.getBatchId());
//					updateConsensusSetting(epoch);
					epoch.setRollback(true);
					decide(epoch);
				} else if (!Arrays.equals(value, epoch.propAndAppValueHash)) {
					// Leader does evil to me only, need to roll back
					LOGGER.error(
							"(computeAccept) I am proc {}, My last regency is {}, Quorum is satisfied, but leader maybe do evil, will goto pre compute rollback branch!",
							topology.getStaticConf().getProcessId(),
							tomLayer.getSynchronizer().getLCManager().getLastReg());
					LOGGER.error(
							"(computeAccept) I am proc {}, my cid is {}, my propose value hash is {}, recv propose value hash is {}, my epoc timestamp is {}",
							topology.getStaticConf().getProcessId(), cid, epoch.propAndAppValueHash, value,
							epoch.getTimestamp());
					// rollback
					getDefaultExecutor().preComputeRollback(cid, epoch.getBatchId());
					// This round of consensus has been rolled back, mark it
					tomLayer.execManager.updateConsensus(tomLayer.getInExec());

//					updateConsensusSetting(epoch);
					epoch.setRollback(true);
					decide(epoch);

					// Pause processing of new messages, Waiting for trigger state transfer
//                tomLayer.requestsTimer.Enabled(false);
//                tomLayer.requestsTimer.stopTimer();

//					if (!tomLayer.execManager.stopped()) {
//						tomLayer.execManager.stop();
//					}
				}
				return;
			}

			// consensus node hash inconsistent
			if (((epoch.countAcceptSetted() == topology.getCurrentViewN())
					&& (epoch.countAccept(value) < topology.getQuorum() + 1))
					|| ((epoch.countAcceptSetted() > 2f) && (epoch.countAccept(value) < topology.getCurrentViewF() + 1)
							&& (epoch.maxSameValueCount() < topology.getCurrentViewF() + 1))) {

				LOGGER.error(
						"Quorum is not satisfied, node's pre compute hash is inconsistent, will goto pre compute rollback phase!");
				getDefaultExecutor().preComputeRollback(cid, epoch.getBatchId());
//				updateConsensusSetting(epoch);

				updatedResp = getDefaultExecutor().updateResponses(epoch.getAsyncResponseLinkedList(), epoch.commonHash,
						false);
				epoch.setAsyncResponseLinkedList(updatedResp);
				epoch.setRollback(true);
				decide(epoch);
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}

	}

	// Reconfig请求
	private boolean isReconfig(TOMMessage tomMessage) {
		return tomMessage.getReqType() == TOMMessageType.RECONFIG;
	}

	/**
	 * This is the method invoked when a value is decided by this process
	 * 
	 * @param epoch Epoch at which the decision is made
	 */
	private void decide(Epoch epoch) {
		if (epoch.getConsensus().getDecision().firstMessageProposed != null)
			epoch.getConsensus().getDecision().firstMessageProposed.decisionTime = System.nanoTime();

		epoch.getConsensus().decided(epoch, true);
	}

	public synchronized void shutdown() {
		doWork = false;

//		if (!doWork) {
//			return;
//		}
//		doWork = false;
//		thrdWork.interrupt();
//		try {
//			thrdWork.join();
//		} catch (InterruptedException e) {
//		}
//		thrdWork = null;
	}

	public synchronized void start() {
		doWork = true;

//		if (doWork) {
//			return;
//		}
//		thrdWork = new Thread(new Runnable() {
//			@Override
//			public void run() {
//				LOGGER.info("Start processing consensus message ... [CurrentProcessId={}]",
//						controller.getCurrentProcessId());
//
//				startProcessPropose();
//
//				LOGGER.info("Exit processing consensus message!! --[CurrentProcessId={}]",
//						controller.getCurrentProcessId());
//			}
//		}, "Acceptor");
//		thrdWork.setDaemon(true);
//
//		doWork = true;
//
//		thrdWork.start();
	}

}
