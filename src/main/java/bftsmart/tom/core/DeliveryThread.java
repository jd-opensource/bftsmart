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
package bftsmart.tom.core;

import bftsmart.consensus.Decision;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.leaderchange.CertifiedDecision;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.util.BatchReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

//import bftsmart.tom.util.Logger;

/**
 * This class implements a thread which will deliver totally ordered requests to
 * the application
 * 
 */
public final class DeliveryThread extends Thread {

	private boolean doWork = true;
	private final LinkedBlockingQueue<Decision> decided;
	private final TOMLayer tomLayer; // TOM layer
	private final ServiceReplica receiver; // Object that receives requests from clients
	private final Recoverable recoverer; // Object that uses state transfer
	private final ServerViewController controller;
	private final Lock decidedLock = new ReentrantLock();
	private final Condition notEmptyQueue = decidedLock.newCondition();

	private static final Logger LOGGER = LoggerFactory.getLogger(DeliveryThread.class);

	/**
	 * Creates a new instance of DeliveryThread
	 * 
	 * @param tomLayer TOM layer
	 * @param receiver Object that receives requests from clients
	 */
	public DeliveryThread(TOMLayer tomLayer, ServiceReplica receiver, Recoverable recoverer,
                          ServerViewController controller) {
		super("Delivery Thread");
		this.decided = new LinkedBlockingQueue<>();

		this.tomLayer = tomLayer;
		this.receiver = receiver;
		this.recoverer = recoverer;
		// ******* EDUARDO BEGIN **************//
		this.controller = controller;
		// ******* EDUARDO END **************//
	}

	public Recoverable getRecoverer() {
		return recoverer;
	}

	public ServiceReplica getReceiver() {
		return receiver;
	}

	/**
	 * Invoked by the TOM layer, to deliver a decision
	 * 
	 * @param dec Decision established from the consensus
	 */
	public void delivery(Decision dec) {
//        if (!containsGoodReconfig(dec)) {

//            LOGGER.debug("(DeliveryThread.delivery) Decision from consensus {} does not contain good reconfiguration", dec.getConsensusId());
		// set this decision as the last one from this replica

		// 此轮共识是否发生过回滚
		if (dec.getRollback()) {
			this.tomLayer.getExecManager().removeSingleConsensus(dec.getConsensusId());
			tomLayer.setInExec(-1);
		} else {
			tomLayer.setLastExec(dec.getConsensusId());
			tomLayer.setInExec(-1);
		}
//            tomLayer.getExecManager().getConsensus(tomLayer.getLastExec()).setPrecomputeCommited(true);
		// define that end of this execution

//        } //else if (tomLayer.controller.getStaticConf().getProcessId() == 0) System.exit(0);
//        else {
//            tomLayer.execManager.removeConsensus(dec.getConsensusId());
		// define that end of this execution
//            tomLayer.setInExec(-1);
//        }
		try {
			decidedLock.lock();
			decided.put(dec);

			// clean the ordered messages from the pending buffer
			TOMMessage[] requests = extractMessagesFromDecision(dec);
			tomLayer.clientsManager.requestsOrdered(requests);

			notEmptyQueue.signalAll();
			decidedLock.unlock();
			LOGGER.debug("(DeliveryThread.delivery) Consensus {}, finished. Decided size {}", dec.getConsensusId(),
					decided.size());
		} catch (Exception e) {
			LOGGER.error("Error occurred while delivering! --[CurrentProcessId=" + tomLayer.getCurrentProcessId() + "]"
					+ e.getMessage(), e);
		}
	}

	private boolean containsGoodReconfig(Decision dec) {
		TOMMessage[] decidedMessages = dec.getDeserializedValue();

		for (TOMMessage decidedMessage : decidedMessages) {
			if (decidedMessage.getReqType() == TOMMessageType.RECONFIG
					|| decidedMessage.getViewID() < this.controller.getCurrentViewId())
//                    && decidedMessage.getViewID() == controller.getCurrentViewId()) {   //缺少节点间的视图同步过程
			{
				return true;
			}
		}
		return false;
	}

	/** THIS IS JOAO'S CODE, TO HANDLE STATE TRANSFER */
	private ReentrantLock deliverLock = new ReentrantLock();
	private Condition canDeliver = deliverLock.newCondition();

	public void deliverLock() {
		// release the delivery lock to avoid blocking on state transfer
		decidedLock.lock();

		notEmptyQueue.signalAll();
		decidedLock.unlock();

		deliverLock.lock();
	}

	public void deliverUnlock() {
		deliverLock.unlock();
	}

	public void canDeliver() {
		canDeliver.signalAll();
	}

	public void update(ApplicationState state) {

		int lastCID = recoverer.setState(state);

		// set this decision as the last one from this replica
		LOGGER.debug("Setting last CID to {}", lastCID);
		tomLayer.setLastExec(lastCID);

		// define the last stable consensus... the stable consensus can
		// be removed from the leaderManager and the executionManager
		if (lastCID > 2) {
			int stableConsensus = lastCID - 3;
			tomLayer.execManager.removeOutOfContexts(stableConsensus);
		}

		// define that end of this execution
		// stateManager.setWaiting(-1);
		tomLayer.setNoExec();

		LOGGER.debug("Current decided size {}", decided.size());
		decided.clear();

		LOGGER.debug("(DeliveryThread.update) All finished up to {}", lastCID);
	}

	/**
	 * This is the code for the thread. It delivers decisions to the TOM request
	 * receiver object (which is the application)
	 */
	@Override
	public void run() {
		while (doWork) {
			/** THIS IS JOAO'S CODE, TO HANDLE STATE TRANSFER */
			deliverLock();
			while (tomLayer.isRetrievingState()) {
				LOGGER.debug("-- Retrieving State");
				canDeliver.awaitUninterruptibly();

				if (tomLayer.getLastExec() == -1)
					LOGGER.debug("-- Ready to process operations");
			}
			try {
				ArrayList<Decision> decisions = new ArrayList<Decision>();
				decidedLock.lock();
				if (decided.isEmpty()) {
					notEmptyQueue.await();
				}

				Decision decision = decided.poll();
				if (decision != null) {
					decisions.add(decision);
				}
				decidedLock.unlock();

				if (!doWork)
					break;

				if (decisions.size() > 0) {
					TOMMessage[][] requests = new TOMMessage[decisions.size()][];
					int[] consensusIds = new int[requests.length];
					int[] leadersIds = new int[requests.length];
					int[] regenciesIds = new int[requests.length];
					List<byte[]> asyncResponseLinkedList = new ArrayList<>();
					CertifiedDecision[] cDecs;
					cDecs = new CertifiedDecision[requests.length];
					int count = 0;
					for (Decision d : decisions) {
						requests[count] = extractMessagesFromDecision(d);
						consensusIds[count] = d.getConsensusId();
						leadersIds[count] = d.getLeader();
						regenciesIds[count] = d.getRegency();

						for (int i = 0; i < d.getDecisionEpoch().getAsyncResponseLinkedList().size(); i++) {
							asyncResponseLinkedList.add(d.getDecisionEpoch().getAsyncResponseLinkedList().get(i));
						}

						CertifiedDecision cDec = new CertifiedDecision(this.controller.getStaticConf().getProcessId(),
								d.getConsensusId(), d.getValue(), d.getDecisionEpoch().proof);
						cDecs[count] = cDec;

						// cons.firstMessageProposed contains the performance counters
						if (requests[count][0].equals(d.firstMessageProposed)) {
							long time = requests[count][0].timestamp;
							long seed = requests[count][0].seed;
							int numOfNonces = requests[count][0].numOfNonces;
							requests[count][0] = d.firstMessageProposed;
							requests[count][0].timestamp = time;
							requests[count][0].seed = seed;
							requests[count][0].numOfNonces = numOfNonces;
						}

						count++;
					}

					Decision lastDecision = decisions.get(decisions.size() - 1);

					if (requests != null && requests.length > 0) {
						deliverMessages(consensusIds, regenciesIds, leadersIds, cDecs, requests,
								asyncResponseLinkedList, lastDecision.getRollback());

						// ******* EDUARDO BEGIN ***********//
						if (controller.hasUpdates()) {
							processReconfigMessages(lastDecision.getConsensusId());

//                            tomLayer.execManager.removeConsensus(lastDecision.getConsensusId());

							// set the consensus associated to the last decision as the last executed
//                            tomLayer.setLastExec(lastDecision.getConsensusId());
							// define that end of this execution
//                            tomLayer.setInExec(-1);
							// ******* EDUARDO END **************//
						}
					}

					// define the last stable consensus... the stable consensus can
					// be removed from the leaderManager and the executionManager
					// TODO: Is this part necessary? If it is, can we put it
					// inside setLastExec
					int cid = lastDecision.getConsensusId();
					if (cid > 2) {
						int stableConsensus = cid - 3;

						tomLayer.execManager.removeConsensus(stableConsensus);
					}
				}
			} catch (Exception e) {
				LOGGER.error("Error occurred while delivering decision! -- " + e.getMessage() + " --[CurrentProccesId="
						+ tomLayer.getCurrentProcessId() + "]", e);
			}

			/** THIS IS JOAO'S CODE, TO HANDLE STATE TRANSFER */
			deliverUnlock();
			/******************************************************************/
		}
		LOGGER.info("DeliveryThread stopped.");

	}

	private TOMMessage[] extractMessagesFromDecision(Decision dec) {
		TOMMessage[] requests = (TOMMessage[]) dec.getDeserializedValue();
		if (requests == null) {
			// there are no cached deserialized requests
			// this may happen if this batch proposal was not verified
			// TODO: this condition is possible?

			LOGGER.debug("(DeliveryThread.run) interpreting and verifying batched requests.");

			// obtain an array of requests from the decisions obtained
			BatchReader batchReader = new BatchReader(dec.getValue(),
					controller.getStaticConf().isUseSignatures());
			requests = batchReader.deserialiseRequests(controller);
		} else {
			LOGGER.debug("(DeliveryThread.run) using cached requests from the propose.");
		}

		return requests;
	}

	protected void deliverUnordered(TOMMessage request, int regency) {

		MessageContext msgCtx = new MessageContext(request.getSender(), request.getViewID(), request.getReqType(),
				request.getSession(), request.getSequence(), request.getOperationId(), request.getReplyServer(),
				request.serializedMessageSignature, System.currentTimeMillis(), 0, 0, regency, -1, -1, null, null,
				false); // Since the request is unordered,
						// there is no consensus info to pass

		msgCtx.readOnly = true;
		receiver.receiveReadonlyMessage(request, msgCtx);
	}

	private void deliverMessages(int consId[], int regencies[], int leaders[], CertifiedDecision[] cDecs,
                                 TOMMessage[][] requests, List<byte[]> asyncResponseLinkedList, boolean isRollback) {
		receiver.receiveMessages(consId, regencies, leaders, cDecs, requests, asyncResponseLinkedList, isRollback);
	}

	private void processReconfigMessages(int consId) {
		byte[] response = controller.executeUpdates(consId);
		TOMMessage[] dests = controller.clearUpdates();

		for (int i = 0; i < dests.length; i++) {
			tomLayer.getCommunication().send(new int[] { dests[i].getSender() },
					new TOMMessage(controller.getStaticConf().getProcessId(), dests[i].getSession(),
							dests[i].getSequence(), dests[i].getOperationId(), response, null, controller.getCurrentViewId(),
							TOMMessageType.RECONFIG));
		}
		if (controller.getCurrentView().isMember(receiver.getId())) {
			tomLayer.getCommunication().updateServersConnections();
		} else {
//            receiver.restart();
//            receiver.kill();
		}
	}

	public void shutdown() {
		this.doWork = false;

		LOGGER.info("Shutting down delivery thread");

		decidedLock.lock();
		notEmptyQueue.signalAll();
		decidedLock.unlock();
	}
}
