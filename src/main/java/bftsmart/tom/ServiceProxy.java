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
package bftsmart.tom;

import bftsmart.reconfiguration.ReconfigureReply;
import bftsmart.reconfiguration.util.TOMConfiguration;
import bftsmart.reconfiguration.views.View;
import bftsmart.reconfiguration.views.ViewStorage;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.util.Extractor;
import bftsmart.tom.util.TOMUtil;
import org.slf4j.LoggerFactory;
import utils.codec.Base58Utils;
import utils.exception.ViewObsoleteException;
import utils.net.SSLSecurity;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class implements a TOMSender and represents a proxy to be used on the
 * client side of the replicated system. It sends a request to the replicas,
 * receives the reply, and delivers it to the application.
 */
public class ServiceProxy extends TOMSender {

	// Locks for send requests and receive replies
	protected ReentrantLock canReceiveLock = new ReentrantLock();
	protected ReentrantLock canSendLock = new ReentrantLock();
	private Semaphore sm = new Semaphore(0);
	private volatile int reqId = -1; // request id
	private int operationId = -1; // request id
	private TOMMessageType requestType;
	private int replyQuorum = 0; // size of the reply quorum
	private TOMMessage replies[] = null; // Replies from replicas are stored here
	private int receivedReplies = 0; // Number of received replies
	private TOMMessage response = null; // Reply delivered to the application
	private int invokeTimeout = 150;
	private Comparator<byte[]> comparator;
	private Extractor extractor;
	private Random rand = new Random(System.currentTimeMillis());
	private int replyServer;
	private HashResponseController hashResponseController;
	private int invokeUnorderedHashedTimeout = 10;
	private boolean viewObsolete = false;
	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ServiceProxy.class);

	public ServiceProxy(TOMConfiguration config, ViewStorage viewStorage, Comparator<byte[]> replyComparator, Extractor replyExtractor, SSLSecurity sslSecurity) {
		init(config, viewStorage, sslSecurity);

		replies = new TOMMessage[getViewManager().getCurrentViewN()];

		comparator = (replyComparator != null) ? replyComparator : new Comparator<byte[]>() {
			@Override
			public int compare(byte[] o1, byte[] o2) {
				return Arrays.equals(o1, o2) ? 0 : -1;
			}
		};

		extractor = (replyExtractor != null) ? replyExtractor : new Extractor() {

			@Override
			public TOMMessage extractResponse(TOMMessage[] replies, int sameContent, int lastReceived) {
				return replies[lastReceived];
			}
		};
	}

	/**
	 * Get the amount of time (in seconds) that this proxy will wait for servers
	 * replies before returning null.
	 *
	 * @return the invokeTimeout
	 */
	public int getInvokeTimeout() {
		return invokeTimeout;
	}

	public int getInvokeUnorderedHashedTimeout() {
		return invokeUnorderedHashedTimeout;
	}

	/**
	 * Set the amount of time (in seconds) that this proxy will wait for servers
	 * replies before returning null.
	 *
	 * @param invokeTimeout
	 *            the invokeTimeout to set
	 */
	public void setInvokeTimeout(int invokeTimeout) {
		this.invokeTimeout = invokeTimeout;
	}

	public void setInvokeUnorderedHashedTimeout(int timeout) {
		this.invokeUnorderedHashedTimeout = timeout;
	}

	public byte[] invokeOrdered(byte[] request) {
		try {
			return invoke(request, TOMMessageType.ORDERED_REQUEST);
		} catch (ViewObsoleteException voe) {
		    close();
			throw voe;
		} catch (Exception e) {
			LOGGER.error("[ServiceProxy] invokeOrdered exception!, error = {}", e.getMessage());
			throw e;
		}
	}

	public byte[] invokeUnordered(byte[] request) {
		return invoke(request, TOMMessageType.UNORDERED_REQUEST);
	}

	public byte[] invokeUnorderedHashed(byte[] request) {
		return invoke(request, TOMMessageType.UNORDERED_HASHED_REQUEST);
	}

	/**
	 * This method sends a request to the replicas, and returns the related reply.
	 * If the servers take more than invokeTimeout seconds the method returns null.
	 * This method is thread-safe.
	 *
	 * @param request
	 *            Request to be sent
	 * @param reqType
	 *            TOM_NORMAL_REQUESTS for service requests, and other for reconfig
	 *            requests.
	 * @return The reply from the replicas related to request
	 */
	public byte[] invoke(byte[] request, TOMMessageType reqType) {
		canSendLock.lock();

		try {
			// Clean all statefull data to prepare for receiving next replies
			Arrays.fill(replies, null);
			receivedReplies = 0;
			response = null;
			replyQuorum = getReplyQuorum();

			// Send the request to the replicas, and get its ID
			reqId = generateRequestId(reqType);
			operationId = generateOperationId();
			requestType = reqType;

			replyServer = -1;
			hashResponseController = null;

			LOGGER.info("Before Sending request {} with reqId {}, operationId {}, clientId={}", reqType, reqId, operationId, getProcessId());

			if (requestType == TOMMessageType.UNORDERED_HASHED_REQUEST) {

				replyServer = getRandomlyServerId();
				LOGGER.debug("[{}] replyServerId {} pos at {}", this.getClass().getName(), replyServer, getViewManager().getCurrentViewPos(replyServer));

				hashResponseController = new HashResponseController(getViewManager().getCurrentViewPos(replyServer),
						getViewManager().getCurrentViewProcesses().length);

				TOMMessage sm = new TOMMessage(getProcessId(), getSession(), reqId, operationId, request, null,
						getViewManager().getCurrentViewId(), requestType);
				sm.setReplyServer(replyServer);

				TOMulticast(sm);
			} else {
				TOMulticast(request, reqId, operationId, reqType);
			}

			LOGGER.info("After Sending request {} with reqId {}, operationId {}, clientId={}", reqType, reqId, operationId, getProcessId());

			LOGGER.debug("Expected number of matching replies: {}", replyQuorum);

			// This instruction blocks the thread, until a response is obtained.
			// The thread will be unblocked when the method replyReceived is invoked
			// by the client side communication system
			try {
				if (reqType == TOMMessageType.UNORDERED_HASHED_REQUEST) {
					if (!this.sm.tryAcquire(invokeUnorderedHashedTimeout, TimeUnit.SECONDS)) {
						LOGGER.debug("######## UNORDERED HASHED REQUEST TIMOUT ########");
						return invoke(request, TOMMessageType.ORDERED_REQUEST);
					}
				} else {
					if (!this.sm.tryAcquire(invokeTimeout, TimeUnit.SECONDS)) {
						LOGGER.error("###################TIMEOUT#######################");
						LOGGER.error("Reply timeout for reqId is {}", reqId);
						LOGGER.error("Process id {} // req id {} // TIMEOUT // ", getProcessId(), reqId);
						LOGGER.error("Replies received: {}", receivedReplies);
						LOGGER.error("Replies quorum: {}", replyQuorum);
						checkReplyNum(reqType, receivedReplies, replyQuorum);
						return null;
					}
				}
			} catch (ViewObsoleteException voe) {
				throw voe;
			} catch (Exception ex) {
				LOGGER.error("exception", ex);
			}

			LOGGER.debug("Response extracted {}", response);

			byte[] ret = null;

			if (response == null) {
				// the response can be null if n-f replies are received but there isn't
				// a replyQuorum of matching replies
				LOGGER.error("Received n-f replies and no response could be extracted. request.length = {}, type = {} !", request.length, reqType);

//				if (reqType == TOMMessageType.UNORDERED_REQUEST || reqType == TOMMessageType.UNORDERED_HASHED_REQUEST) {
//					// invoke the operation again, whitout the read-only flag
//					LOGGER.debug("###################RETRY#######################");
//					return invokeUnordered(request);
//				} else {
//					throw new RuntimeException("Received n-f replies without f+1 of them matching.");
//				}

				throw new RuntimeException("Received n-f replies without f+1 of them matching.");

			} else {
				// normal operation
				// ******* EDUARDO BEGIN **************//
				if (reqType == TOMMessageType.ORDERED_REQUEST) {
					// Reply to a normal request!
					if (response.getViewID() == getViewManager().getCurrentViewId()) {
						ret = response.getContent(); // return the response
					} else {// if(response.getViewID() > getViewManager().getCurrentViewId())
						// Request with backward viewid, still up chain by server node, but proxy client need update local view according to response view
						LOGGER.warn("Service proxy view id little than service replica view id, will update local view!");
						if (response.getViewContent() != null) {
							reconfigureTo((View) TOMUtil.getObject(response.getViewContent()));
						}
						ret = response.getContent();

//						LOGGER.warn("Service proxy view id little than service replica view id, will re invoke request!");
//						return invoke(request, reqType);

					}
				} else if (reqType == TOMMessageType.UNORDERED_REQUEST
						|| reqType == TOMMessageType.UNORDERED_HASHED_REQUEST) {
					ret = response.getContent(); // return the response
					if (response.getViewID() > getViewManager().getCurrentViewId()) {
						Object r = TOMUtil.getObject(response.getContent());
						if (r instanceof View) {
							reconfigureTo((View) r);
							return invoke(request, reqType);
						}
					}
				} else {
					if (response.getViewID() > getViewManager().getCurrentViewId()) {
						// Reply to a reconfigure request!
						LOGGER.debug("Reconfiguration request' reply received!");
						Object r = TOMUtil.getObject(response.getContent());
						if (r instanceof View) { // did not executed the request because it is using an outdated view
							reconfigureTo((View) r);

							return invoke(request, reqType);
						} else if (r instanceof ReconfigureReply) { // reconfiguration executed!
							reconfigureTo(((ReconfigureReply) r).getView());
							ret = response.getContent();
						} else {
							LOGGER.error("Unknown response type");
						}
					} else {
						LOGGER.error("Unexpected execution flow");
					}
				}
			}
			return ret;
		} catch (Exception e) {
			LOGGER.error("[ServiceProxy] invoke exception occur!", e);
			throw e;
		} finally {
			canSendLock.unlock();
		}
	}

	private void checkReplyNum(TOMMessageType reqType, int receivedReplies, int replyQuorum) {
//		if (reqType == TOMMessageType.ORDERED_REQUEST) {
			LOGGER.info("checkReplyNum, receivedReplies = {}, replyQuorum = {}, viewObsolete = {}", receivedReplies, replyQuorum, viewObsolete);
			if (receivedReplies > 0 && receivedReplies < replyQuorum && viewObsolete) {
				LOGGER.info("################################################################################################################################");
				LOGGER.info("########Consensus Client Recv Reply Num Is Not Satisfy Quorum, Client View Is Obsolete, Please Try To Re-auth Peer Node!########");
				LOGGER.info("################################################################################################################################");
				throw new ViewObsoleteException("Consensus Client View Obsolete, Please Try To Re Conn!");
			} else if (receivedReplies == 0) {
				LOGGER.info("####################################################################################################################################################");
				LOGGER.info("########Consensus Client Recv Reply Num Is 0, Client View Is Serious Obsolete or Consensus Node Block, Please Restart All Nodes And Gateway!########");
				LOGGER.info("####################################################################################################################################################");
			}
//		}
	}

	// ******* EDUARDO BEGIN **************//
	protected void reconfigureTo(View v) {
		LOGGER.debug("Installing a most up-to-date view with id {}", v.getId());
		getViewManager().reconfigureTo(v);
		getViewManager().getViewStore().storeView(v);
		replies = new TOMMessage[getViewManager().getCurrentViewN()];
		getCommunicationSystem().updateConnections();
	}
	// ******* EDUARDO END **************//

	/**
	 * This is the method invoked by the client side communication system.
	 *
	 * @param reply
	 *            The reply delivered by the client side communication system
	 */
	@Override
	public void replyReceived(TOMMessage reply) {
		LOGGER.info("Synchronously received reply from {} with sequence number {} ", reply.getSender(), reply.getSequence());
		canReceiveLock.lock();
		try {
			if (reqId == -1) {// no message being expected
				LOGGER.info("throwing out request: sender {}, reqId {}", reply.getSender(), reply.getSequence());
				return;
			}

			int pos = getViewManager().getCurrentViewPos(reply.getSender());

			if (pos < 0) { // ignore messages that don't come from replicas

				LOGGER.info("received reply from sender {}", reply.getSender());

				return;
			}

			int sameContent = 1;
			if (reply.getSequence() == reqId && reply.getReqType() == requestType) {

				LOGGER.info("I am proc {}, Receiving reply from {} with reqId {}. Putting on pos {}", this.getProcessId(), reply.getSender(), reply.getSequence(), pos);

				if (requestType == TOMMessageType.UNORDERED_HASHED_REQUEST) {
					response = hashResponseController.getResponse(pos, reply);
					if (response != null) {
						reqId = -1;
						this.sm.release(); // resumes the thread that is executing the "invoke" method
						return;
					}

				} else {
					if (this.getViewManager().getCurrentView().getId() < reply.getViewID()) {
						viewObsolete = true;
					}
					if (replies[pos] == null) {
						receivedReplies++;
					}
					replies[pos] = reply;

					// Compare the reply just received, to the others

					for (int i = 0; i < replies.length; i++) {

						if ((i != pos || getViewManager().getCurrentViewN() == 1) && replies[i] != null
								&& (comparator.compare(replies[i].getContent(), reply.getContent()) == 0)) {
							sameContent++;

							LOGGER.info("sameContent = {}, replyQuorum = {}, request type = {}", sameContent, replyQuorum, replies[i].getReqType());

							if (sameContent >= replyQuorum) {

								response = extractor.extractResponse(replies, sameContent, pos);
								reqId = -1;
								viewObsolete = false;
								this.sm.release(); // resumes the thread that is executing the "invoke" method
								return;
							}
						}
					}
				}

				if (response == null) {
					if (requestType.equals(TOMMessageType.ORDERED_REQUEST)) {
						if (receivedReplies == getViewManager().getCurrentViewN()) {
							reqId = -1;
							viewObsolete = false;
							this.sm.release(); // resumes the thread that is executing the "invoke" method
						}
					} else if (requestType.equals(TOMMessageType.UNORDERED_HASHED_REQUEST)) {
						if (hashResponseController.getNumberReplies() == getViewManager().getCurrentViewN()) {
							reqId = -1;
							viewObsolete = false;
							this.sm.release(); // resumes the thread that is executing the "invoke" method
						}
					} else if (requestType.equals(TOMMessageType.UNORDERED_REQUEST)) {
						// UNORDERED 消息
						if (receivedReplies == getViewManager().getCurrentViewN()) {
							reqId = -1;
							viewObsolete = false;
							this.sm.release(); // resumes the thread that is executing the "invoke" method
						}
					} else if (requestType.equals(TOMMessageType.RECONFIG)) {
						if (receivedReplies == getViewManager().getCurrentViewN()) {
							reqId = -1;
							viewObsolete = false;
							this.sm.release(); // resumes the thread that is executing the "invoke" method
						}
					} else { // OTHER
						if (receivedReplies != sameContent) {
							reqId = -1;
							viewObsolete = false;
							this.sm.release(); // resumes the thread that is executing the "invoke" method
						}
					}
				}
			} else {
				LOGGER.info("Ignoring reply from {} with reqId {}. Currently wait reqId {}", reply.getSender(), reply.getSequence(), reqId);
			}
		} catch (Exception ex) {
			LOGGER.error("Problem at ServiceProxy.ReplyReceived()", ex);
		} finally {
			canReceiveLock.unlock();
		}
	}

	protected int getReplyQuorum() {
		if (getViewManager().getStaticConf().isBFT()) {
//			return (int) Math.ceil((getViewManager().getCurrentViewN() + getViewManager().getCurrentViewF()) / 2) + 1;
			return getViewManager().getCurrentViewF() + 1;
		} else {
			return (int) Math.ceil((getViewManager().getCurrentViewN()) / 2) + 1;
		}
	}

	private int getRandomlyServerId() {
		int numServers = super.getViewManager().getCurrentViewProcesses().length;
		int pos = rand.nextInt(numServers);

		return super.getViewManager().getCurrentViewProcesses()[pos];
	}

	private class HashResponseController {
		private TOMMessage reply;
		private byte[][] hashReplies;
		private int replyServerPos;
		private int countHashReplies;

		public HashResponseController(int replyServerPos, int length) {
			this.replyServerPos = replyServerPos;
			this.hashReplies = new byte[length][];
			this.reply = null;
			this.countHashReplies = 0;
		}

		public TOMMessage getResponse(int pos, TOMMessage tomMessage) {

			if (hashReplies[pos] == null) {
				countHashReplies++;
			}

			if (replyServerPos == pos) {
				reply = tomMessage;
				try {
					hashReplies[pos] = TOMUtil.computeHash(tomMessage.getContent());
				} catch (NoSuchAlgorithmException e) {
					LOGGER.error("hash exception", e);
				}
			} else {
				hashReplies[pos] = tomMessage.getContent();
			}
			LOGGER.debug("[{}] hashReplies[{}] = {}", this.getClass().getName(), pos, Arrays.toString(hashReplies[pos]));

			if (hashReplies[replyServerPos] != null) {
				int sameContent = 1;
				for (int i = 0; i < replies.length; i++) {
					if ((i != replyServerPos || getViewManager().getCurrentViewN() == 1) && hashReplies[i] != null
							&& (Arrays.equals(hashReplies[i], hashReplies[replyServerPos]))) {
						sameContent++;
						if (sameContent >= replyQuorum) {
							return reply;
						}
					}
				}
			}
			return null;
		}

		public int getNumberReplies() {
			return countHashReplies;
		}
	}
}
