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
package bftsmart.tom;

import bftsmart.communication.MessageHandler;
import bftsmart.communication.ServerCommunicationSystem;
import bftsmart.communication.ServerCommunicationSystemImpl;
import bftsmart.communication.client.ClientCommunicationFactory;
import bftsmart.communication.client.ClientCommunicationServerSide;
import bftsmart.consensus.app.PreComputeBatchExecutable;
import bftsmart.consensus.messages.MessageFactory;
import bftsmart.consensus.roles.Acceptor;
import bftsmart.consensus.roles.Proposer;
import bftsmart.reconfiguration.ReconfigureRequest;
import bftsmart.reconfiguration.ReplicaTopology;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.reconfiguration.util.TOMConfiguration;
import bftsmart.reconfiguration.views.FileSystemViewStorage;
import bftsmart.reconfiguration.views.MemoryBasedViewStorage;
import bftsmart.reconfiguration.views.NodeNetwork;
import bftsmart.reconfiguration.views.View;
import bftsmart.reconfiguration.views.ViewStorage;
import bftsmart.tom.core.ExecutionManager;
import bftsmart.tom.core.ReplyManager;
import bftsmart.tom.core.TOMLayer;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.leaderchange.CertifiedDecision;
import bftsmart.tom.server.Executable;
import bftsmart.tom.server.FIFOExecutable;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.server.Replier;
import bftsmart.tom.server.RequestVerifier;
import bftsmart.tom.server.SingleExecutable;
import bftsmart.tom.server.defaultservices.DefaultReplier;
import bftsmart.tom.util.ShutdownHookThread;
import bftsmart.tom.util.TOMUtil;
import utils.net.SSLSecurity;
import org.slf4j.LoggerFactory;
import utils.codec.Base58Utils;

import java.io.File;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/**
 * This class receives messages from DeliveryThread and manages the execution
 * from the application and reply to the clients. For applications where the
 * ordered messages are executed one by one, ServiceReplica receives the batch
 * decided in a consensus, deliver one by one and reply with the batch of
 * replies. In cases where the application executes the messages in batches, the
 * batch of messages is delivered to the application and ServiceReplica doesn't
 * need to organize the replies in batches.
 */
public class ServiceReplica {

	class MessageContextPair {

		TOMMessage message;
		MessageContext msgCtx;

		MessageContextPair(TOMMessage message, MessageContext msgCtx) {
			this.message = message;
			this.msgCtx = msgCtx;
		}
	}

	// replica ID
	private final int id;
	// Server side comunication system
	private ServerCommunicationSystem cs = null;
	private ReplyManager repMan = null;
	private ServerViewController serverViewController;
	private ReentrantLock waitTTPJoinMsgLock = new ReentrantLock();
	private Condition canProceed = waitTTPJoinMsgLock.newCondition();
	private final Executable executor;
	private Recoverable recoverer = null;
//	private TOMLayer tomLayer = null;
	private volatile boolean tomStackCreated = false;
	private ReplicaContext replicaCtx = null;
	private Replier replier = null;
	private RequestVerifier verifier = null;
	private final String realmName;
	private long lastCid;
	private ClientCommunicationServerSide clientCommunication;
	private MessageHandler messageHandler;
	private SSLSecurity sslSecurity;

//	private Acceptor acceptor;

//	private HeartBeatTimer heartBeatTimer = null;

	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ServiceReplica.class);

	public ServiceReplica(TOMConfiguration config, Executable executor, Recoverable recoverer) {
		this(null, null, new ServerViewController(config, new MemoryBasedViewStorage()), executor, recoverer, null,
				new DefaultReplier(), -1, "Default-Realm");
	}

	public ServiceReplica(TOMConfiguration config, String runtimeDir, Executable executor, Recoverable recoverer) {
		this(null, null, new ServerViewController(config, new FileSystemViewStorage(null, new File(runtimeDir, "view"))), executor,
				recoverer, null, new DefaultReplier(), -1, "Default-Realm");
	}

	public ServiceReplica(TOMConfiguration config, View initView, String runtimeDir, Executable executor,
                          Recoverable recoverer, RequestVerifier verifier, Replier replier) {
		this(null, null, new ServerViewController(config, new FileSystemViewStorage(initView, new File(runtimeDir, "view"))),
				executor, recoverer, verifier, replier, -1, "Default-Realm");
	}

	public ServiceReplica(TOMConfiguration config, ViewStorage viewStorage, Executable executor, Recoverable recoverer,
                          RequestVerifier verifier, Replier replier) {
		this(null, null, new ServerViewController(config, viewStorage), executor, recoverer, verifier, replier, -1,
				"Default-Realm");
	}

	public ServiceReplica(MessageHandler messageHandler, ServerCommunicationSystem cs, TOMConfiguration config, Executable executor, Recoverable recoverer, long lastCid,
                          View lastView, String realName) {
		this(messageHandler, cs, new ServerViewController(config, new MemoryBasedViewStorage(lastView)), executor, recoverer, null,
				new DefaultReplier(), (int) lastCid, realName, null);
	}

	public ServiceReplica(MessageHandler messageHandler, ServerCommunicationSystem cs, TOMConfiguration config, Executable executor, Recoverable recoverer, int lastCid,
						  View lastView, String realName, SSLSecurity sslSecurity) {
		this(messageHandler, cs, new ServerViewController(config, new MemoryBasedViewStorage(lastView)), executor, recoverer, null,
				new DefaultReplier(), lastCid, realName, sslSecurity);
	}

	protected ServiceReplica(MessageHandler messageHandler, ServerCommunicationSystem cs, ServerViewController viewController, Executable executor, Recoverable recoverer,
							 RequestVerifier verifier, Replier replier, int lastCid, String realName) {
		this(messageHandler, cs, viewController, executor, recoverer, verifier, replier, lastCid, realName, new SSLSecurity());
	}

	protected ServiceReplica(MessageHandler messageHandler, ServerCommunicationSystem cs, ServerViewController viewController, Executable executor, Recoverable recoverer,
			RequestVerifier verifier, Replier replier, int lastCid, String realName, SSLSecurity sslSecurity) {
		this.id = viewController.getStaticConf().getProcessId();
		this.realmName = realName;
		this.serverViewController = viewController;
		this.executor = executor;
		this.recoverer = recoverer;
		this.replier = (replier != null ? replier : new DefaultReplier());
		this.verifier = verifier;
		this.recoverer.setRealName(realName);
		this.messageHandler = messageHandler;
		this.cs = cs;
        this.lastCid = lastCid;
		this.sslSecurity = sslSecurity;

//		if (viewController.getStaticConf().isLoggingToDisk()) {
//			this.lastCid = this.recoverer.getStateManager().getLastCID();
//		} else {
//			this.lastCid = lastCid;
//		}

		try {
			this.replicaCtx = this.init();

			// 先启动通讯层，以便其它部分在启动后的通讯操作能够正常进行；
			this.cs.start();
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
			}

			this.replier.initContext(replicaCtx);
			this.recoverer.initContext(replicaCtx, lastCid);

			startReplica(replicaCtx);
		} catch (Throwable e) {
		    LOGGER.error("[ServiceReplica] start exception!, error = {}", e);
			throw e;
		}
	}

	public void setReplyController(Replier replier) {
		this.replier = replier;
	}

	public ReplicaTopology getViewController() {
		return serverViewController;
	}

	public Executable getExecutor() {
		return executor;
	}

	public Replier getReplier() {
		return replier;
	}

	public ReplyManager getRepMan() {
		return repMan;
	}

	// this method initializes the object
	private ReplicaContext init() {
		try {
			if (messageHandler == null) {
				messageHandler = new MessageHandler();
			}

			if (cs == null) {
				clientCommunication = ClientCommunicationFactory.createServerSide(serverViewController, sslSecurity);
				cs = new ServerCommunicationSystemImpl(clientCommunication, messageHandler, this.serverViewController,
						realmName, sslSecurity);
			} else {
				clientCommunication = cs.getClientCommunication();
			}
		} catch (Throwable ex) {
			throw new RuntimeException("Unable to build a communication system.", ex);
		}

		if (!this.serverViewController.isInCurrentView()) {
//			LOGGER.error("Current replica is not in current view! --[ReplicaId={}][View={}]",
//					serverViewController.getCurrentProcessId(), this.serverViewController.getCurrentView());
//			// Not in the initial view, just waiting for the view where the join has been
//			// executed
//			LOGGER.error("-- Waiting for the TTP: {}", this.serverViewController.getCurrentView());
//			waitTTPJoinMsgLock.lock();
//			try {
//				canProceed.awaitUninterruptibly();
//			} finally {
//				waitTTPJoinMsgLock.unlock();
//			}

			throw new IllegalStateException(
					String.format("Current replica is not in current view! --[ReplicaId=%s][View=%s]",
							serverViewController.getCurrentProcessId(), serverViewController.getCurrentView()));
		}
		
		ReplicaContext context = initTOMLayer(id, realmName, this, cs, recoverer, serverViewController, verifier,
				messageHandler, clientCommunication); // initiaze the TOM layer
		return context;
		
	}

//	public void joinMsgReceived(VMMessage msg) {
//		ReconfigureReply r = msg.getReply();
//
//		if (r.getView().isMember(id)) {
//			this.serverViewController.processJoinResult(r);
//
//			if (!tomStackCreated) { // if this object was already initialized, don't do it again
//				replicaCtx = initTOMLayer(id, realmName, this, cs, recoverer, serverViewController, lastCid, verifier,
//						messageHandler, clientCommunication); // initiaze
//				// the
//				// TOM
//				// layer
//				tomStackCreated = true;
//			}
//			cs.updateServersConnections();
//			this.cs.joinViewReceived();
//			waitTTPJoinMsgLock.lock();
//			canProceed.signalAll();
//			waitTTPJoinMsgLock.unlock();
//		}
//	}

	private void startReplica(ReplicaContext context) {
		repMan = new ReplyManager(serverViewController.getStaticConf().getNumRepliers(), cs);

		context.start();
	}

	/**
	 * This message delivers a readonly message, i.e., a message that was not
	 * ordered to the replica and gather the reply to forward to the client
	 *
	 * @param message the request received from the delivery thread
	 */
	public final void receiveReadonlyMessage(TOMMessage message, MessageContext msgCtx) {
		byte[] response = null;

		// This is used to deliver the requests to the application and obtain a reply to
		// deliver
		// to the clients. The raw decision does not need to be delivered to the
		// recoverable since
		// it is not associated with any consensus instance, and therefore there is no
		// need for
		// applications to log it or keep any proof.
		if (executor instanceof FIFOExecutable) {
			response = ((FIFOExecutable) executor).executeUnorderedFIFO(message.getContent(), msgCtx,
					message.getSender(), message.getOperationId());
		} else {
			if (message.getViewID() == serverViewController.getCurrentViewId()) {
				response = executor.executeUnordered(message.getContent(), msgCtx);
			} else if (message.getViewID() < serverViewController.getCurrentViewId()) {
				View view = serverViewController.getCurrentView();
				List<NodeNetwork> addressesTemp = new ArrayList<>();
				for (int i = 0; i < view.getProcesses().length; i++) {
					int cpuId = view.getProcesses()[i];
					NodeNetwork inetSocketAddress = view.getAddress(cpuId);
					if (inetSocketAddress.getHost().equals("0.0.0.0")) {
						// proc docker env
						String host = serverViewController.getStaticConf().getOuterHostConfig().getHost(cpuId);
						NodeNetwork tempSocketAddress = new NodeNetwork(host, inetSocketAddress.getConsensusPort(),
								inetSocketAddress.getMonitorPort(), inetSocketAddress.isConsensusSecure(), inetSocketAddress.isMonitorSecure());
						LOGGER.info("I am proc {}, tempSocketAddress.getAddress().getHostAddress() = {}",
								serverViewController.getStaticConf().getProcessId(), host);
						addressesTemp.add(tempSocketAddress);
					} else {
						LOGGER.info("I am proc {}, tempSocketAddress.getAddress().getHostAddress() = {}",
								serverViewController.getStaticConf().getProcessId(), inetSocketAddress);
						addressesTemp.add(inetSocketAddress);
					}
				}

				View replyView = new View(view.getId(), view.getProcesses(), view.getF(),
						addressesTemp.toArray(new NodeNetwork[addressesTemp.size()]));
				response = TOMUtil.getBytes(replyView);
			}
		}

		if (message.getReqType() == TOMMessageType.UNORDERED_HASHED_REQUEST && message.getReplyServer() != this.id) {
			try {
				response = TOMUtil.computeHash(response);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}

		// Generate the messages to send back to the clients
		message.reply = new TOMMessage(id, message.getSession(), message.getSequence(), message.getOperationId(),
				response, null, serverViewController.getCurrentViewId(), message.getReqType());

		if (serverViewController.getStaticConf().getNumRepliers() > 0) {
			repMan.send(message);
		} else {
			cs.send(new int[] { message.getSender() }, message.reply);
		}
	}

	public void kill() {

		Thread t = new Thread() {

			@Override
			public void run() {
//				if (tomLayer != null) {
//					tomLayer.shutdown();
//				}
				if (replicaCtx != null) {
					replicaCtx.shutdown();
					replicaCtx = null;
					tomStackCreated = false;
				}
			}
		};
		t.start();
		try {
			t.join();
		} catch (InterruptedException e) {
		}
	}

	public void restart() {
		Thread t = new Thread() {

			@Override
			public void run() {
				if (replicaCtx != null) {
					replicaCtx.shutdown();

					tomStackCreated = false;
					replicaCtx = null;
					cs = null;

					init();

					recoverer.initContext(replicaCtx, lastCid);
					replier.initContext(replicaCtx);

					replicaCtx.start();
				}
			}
		};
		t.start();
	}

	public void receiveMessages(int consId[], int regencies[], int leaders[], CertifiedDecision[] cDecs,
                                TOMMessage[][] requests, List<byte[]> asyncResponseLinkedList, boolean isRollback) {
		int numRequests = 0;
		int consensusCount = 0;
		List<TOMMessage> toBatch = new ArrayList<>();
		List<MessageContext> msgCtxts = new ArrayList<>();
		boolean noop = true;

		for (TOMMessage[] requestsFromConsensus : requests) {

			TOMMessage firstRequest = requestsFromConsensus[0];
			int requestCount = 0;
			noop = true;
			for (TOMMessage request : requestsFromConsensus) {

				LOGGER.info(
						"(ServiceReplica.receiveMessages) request view id = {}, curr view id = {}, request type = {}",
						request.getViewID(), serverViewController.getCurrentViewId(), request.getReqType());

				// 暂时没有节点间的视图ID同步过程，在处理RECONFIG这类更新视图的操作时先不考虑视图ID落后的情况
				if (request.getViewID() <= serverViewController.getCurrentViewId()) {
//						|| request.getReqType() == TOMMessageType.RECONFIG) {

					if (request.getReqType() == TOMMessageType.ORDERED_REQUEST || request.getReqType() == TOMMessageType.RECONFIG) {
						noop = false;
						numRequests++;
						MessageContext msgCtx = new MessageContext(request.getSender(), request.getViewID(),
								request.getReqType(), request.getSession(), request.getSequence(),
								request.getOperationId(), request.getReplyServer(), request.serializedMessageSignature,
								firstRequest.timestamp, request.numOfNonces, request.seed, regencies[consensusCount],
								leaders[consensusCount], consId[consensusCount],
								cDecs[consensusCount].getConsMessages(), firstRequest, false);

						if (requestCount + 1 == requestsFromConsensus.length) {

							msgCtx.setLastInBatch();
						}
						request.deliveryTime = System.nanoTime();
						if (executor instanceof PreComputeBatchExecutable) {

							LOGGER.debug("(ServiceReplica.receiveMessages) Batching request from {}",
									request.getSender());

							// This is used to deliver the content decided by a consensus instance directly
							// to
							// a Recoverable object. It is useful to allow the application to create a log
							// and
							// store the proof associated with decisions (which are needed by replicas
							// that are asking for a state transfer).
//							if (this.recoverer != null)
//								this.recoverer.Op(msgCtx.getConsensusId(), request.getContent(), msgCtx);

							// deliver requests and contexts to the executor later
							msgCtxts.add(msgCtx);
							toBatch.add(request);
							if (request.getReqType() == TOMMessageType.RECONFIG) {
								serverViewController.enqueueUpdate(request);
							}
						} else if (executor instanceof FIFOExecutable) {

							LOGGER.debug(
									"(ServiceReplica.receiveMessages) Delivering request from {} via FifoExecutable",
									request.getSender());

							// This is used to deliver the content decided by a consensus instance directly
							// to
							// a Recoverable object. It is useful to allow the application to create a log
							// and
							// store the proof associated with decisions (which are needed by replicas
							// that are asking for a state transfer).
							if (this.recoverer != null)
								this.recoverer.Op(msgCtx.getConsensusId(), request.getContent(), msgCtx);

							// This is used to deliver the requests to the application and obtain a reply to
							// deliver
							// to the clients. The raw decision is passed to the application in the line
							// above.
							byte[] response = ((FIFOExecutable) executor).executeOrderedFIFO(request.getContent(),
									msgCtx, request.getSender(), request.getOperationId());

							// Generate the messages to send back to the clients
							request.reply = new TOMMessage(id, request.getSession(), request.getSequence(),
									request.getOperationId(), response, null, serverViewController.getCurrentViewId(),
									request.getReqType());
							LOGGER.debug("(ServiceReplica.receiveMessages) sending reply to {}", request.getSender());
							replier.manageReply(request, msgCtx);
						} else if (executor instanceof SingleExecutable) {

							LOGGER.debug(
									"(ServiceReplica.receiveMessages) Delivering request from {} via SingleExecutable",
									request.getSender());

							// This is used to deliver the content decided by a consensus instance directly
							// to
							// a Recoverable object. It is useful to allow the application to create a log
							// and
							// store the proof associated with decisions (which are needed by replicas
							// that are asking for a state transfer).
							if (this.recoverer != null)
								this.recoverer.Op(msgCtx.getConsensusId(), request.getContent(), msgCtx);

							// This is used to deliver the requests to the application and obtain a reply to
							// deliver
							// to the clients. The raw decision is passed to the application in the line
							// above.
							byte[] response = ((SingleExecutable) executor).executeOrdered(request.getContent(),
									msgCtx);

							// Generate the messages to send back to the clients
							request.reply = new TOMMessage(id, request.getSession(), request.getSequence(),
									request.getOperationId(), response, null, serverViewController.getCurrentViewId(),
									request.getReqType());
							LOGGER.debug("(ServiceReplica.receiveMessages) sending reply to {}", request.getSender());
							replier.manageReply(request, msgCtx);
						} else {
							throw new UnsupportedOperationException("Non-existent interface");
						}
					}
// 						else if (request.getReqType() == TOMMessageType.RECONFIG) {
//						noop = false;
//						numRequests++;
//						serverViewController.enqueueUpdate(request);
//					}
 					else {
						throw new RuntimeException("Should never reach here!");
					}
				}
//				else if (request.getViewID() < serverViewController.getCurrentViewId()) { // message sender had an old
//																							// view,
//					// resend the message to
//					// him (but only if it came from
//					// consensus an not state transfer)
//					View view = serverViewController.getCurrentView();
//
//					List<NodeNetwork> addressesTemp = new ArrayList<>();
//
//					for (int i = 0; i < view.getProcesses().length; i++) {
//						int cpuId = view.getProcesses()[i];
//						NodeNetwork inetSocketAddress = view.getAddress(cpuId);
//
//						if (inetSocketAddress.getHost().equals("0.0.0.0")) {
//							// proc docker env
//							String host = serverViewController.getStaticConf().getOuterHostConfig().getHost(cpuId);
//
//							NodeNetwork tempSocketAddress = new NodeNetwork(host, inetSocketAddress.getConsensusPort(),
//									-1);
//							LOGGER.info("I am proc {}, tempSocketAddress.getAddress().getHostAddress() = {}",
//									serverViewController.getStaticConf().getProcessId(), host);
//							addressesTemp.add(tempSocketAddress);
//						} else {
//							LOGGER.info("I am proc {}, tempSocketAddress.getAddress().getHostAddress() = {}",
//									serverViewController.getStaticConf().getProcessId(), inetSocketAddress.toUrl());
//							addressesTemp.add(new NodeNetwork(inetSocketAddress.getHost(),
//									inetSocketAddress.getConsensusPort(), -1));
//						}
//					}
//
//					View replyView = new View(view.getId(), view.getProcesses(), view.getF(),
//							addressesTemp.toArray(new NodeNetwork[addressesTemp.size()]));
//					LOGGER.info("I am proc {}, view = {}, hashCode = {}, reply View = {}",
//							this.serverViewController.getStaticConf().getProcessId(), view, view.hashCode(), replyView);
//
//					getTomLayer().getCommunication().send(new int[] { request.getSender() },
//							new TOMMessage(serverViewController.getStaticConf().getProcessId(), request.getSession(),
//									request.getSequence(), request.getOperationId(), TOMUtil.getBytes(replyView),
//									serverViewController.getCurrentViewId(), request.getReqType()));
//				}
				requestCount++;
			} // End of : for (TOMMessage request : requestsFromConsensus);

			// This happens when a consensus finishes but there are no requests to deliver
			// to the application. This can happen if a reconfiguration is issued and is the
			// only
			// operation contained in the batch. The recoverer must be notified about this,
			// hence the invocation of "noop"
			if (noop && this.recoverer != null) {

				LOGGER.debug(
						"(ServiceReplica.receiveMessages) I am proc {}, host = {}, port = {}. Delivering a no-op to the recoverer",
						this.serverViewController.getStaticConf().getProcessId(),
						this.serverViewController.getStaticConf()
								.getRemoteAddress(this.serverViewController.getStaticConf().getProcessId()).getHost(),
						this.serverViewController.getStaticConf()
								.getRemoteAddress(this.serverViewController.getStaticConf().getProcessId())
								.getConsensusPort());

				LOGGER.debug(
						"I am proc {} , host = {}, port = {}.--- A consensus instance finished, but there were no commands to deliver to the application.",
						this.serverViewController.getStaticConf().getProcessId(),
						this.serverViewController.getStaticConf()
								.getRemoteAddress(this.serverViewController.getStaticConf().getProcessId()).getHost(),
						this.serverViewController.getStaticConf()
								.getRemoteAddress(this.serverViewController.getStaticConf().getProcessId())
								.getConsensusPort());
				LOGGER.debug("I am proc {} , host = {}, port = {}.--- Notifying recoverable about a blank consensus.",
						this.serverViewController.getStaticConf().getProcessId(),
						this.serverViewController.getStaticConf()
								.getRemoteAddress(this.serverViewController.getStaticConf().getProcessId()).getHost(),
						this.serverViewController.getStaticConf()
								.getRemoteAddress(this.serverViewController.getStaticConf().getProcessId())
								.getConsensusPort());

				byte[][] batch = null;
				MessageContext[] msgCtx = null;
				if (requestsFromConsensus.length > 0) {
					// Make new batch to deliver
					batch = new byte[requestsFromConsensus.length][];
					msgCtx = new MessageContext[requestsFromConsensus.length];

					// Put messages in the batch
					int line = 0;
					for (TOMMessage m : requestsFromConsensus) {
						batch[line] = m.getContent();

						msgCtx[line] = new MessageContext(m.getSender(), m.getViewID(), m.getReqType(), m.getSession(),
								m.getSequence(), m.getOperationId(), m.getReplyServer(), m.serializedMessageSignature,
								firstRequest.timestamp, m.numOfNonces, m.seed, regencies[consensusCount],
								leaders[consensusCount], consId[consensusCount],
								cDecs[consensusCount].getConsMessages(), firstRequest, true);
						msgCtx[line].setLastInBatch();

						line++;
					}
				}

				this.recoverer.noOp(consId[consensusCount], batch, msgCtx);

				// MessageContext msgCtx = new MessageContext(-1, -1, null, -1, -1, -1, -1,
				// null, // Since it is a noop, there is no need to pass info about the
				// client...
				// -1, 0, 0, regencies[consensusCount], leaders[consensusCount],
				// consId[consensusCount], cDecs[consensusCount].getConsMessages(), //... but
				// there is still need to pass info about the consensus
				// null, true); // there is no command that is the first of the batch, since it
				// is a noop
				// msgCtx.setLastInBatch();

				// this.recoverer.noOp(msgCtx.getConsensusId(), msgCtx);
			}

			consensusCount++;
		} // End of: for (TOMMessage[] requestsFromConsensus : requests)

		if (executor instanceof PreComputeBatchExecutable && numRequests > 0) {
			// Make new batch to deliver
			byte[][] batch = new byte[numRequests][];

//			ReplyContext replyContext = new ReplyContext().buildId(id)
//					.buildCurrentViewId(serverViewController.getCurrentViewId())
//					.buildNumRepliers(serverViewController.getStaticConf().getNumRepliers()).buildRepMan(repMan)
//					.buildReplier(replier);

//			List<ReplyContextMessage> replyContextMessages = new ArrayList<>();

			// Put messages in the batch
			int line = 0;
			for (TOMMessage m : toBatch) {
//				replyContextMessages.add(new ReplyContextMessage(replyContext, m));

				if (m.getReqType() == TOMMessageType.RECONFIG) {
					// 对于reconfig类型的消息，扩展消息才是交易本身
					batch[line] = ((ReconfigureRequest) TOMUtil.getObject(m.getContent())).getExtendInfo();
				} else {
					batch[line] = m.getContent();
				}
				line++;
			}

			MessageContext[] msgContexts = new MessageContext[msgCtxts.size()];
			msgContexts = msgCtxts.toArray(msgContexts);

			// Deliver the batch and wait for replies
			if (isRollback == false) {
				byte[][] replies = ((PreComputeBatchExecutable) executor).executeBatch(batch, msgContexts);
			}

			if (toBatch.size() != asyncResponseLinkedList.size()) {
				LOGGER.debug("(ServiceReplica.receiveMessages) toBatch.size() != asyncResponseLinkedList.size()");
				return;
			}
			// Send the replies back to the client
			for (int index = 0; index < toBatch.size(); index++) {
				TOMMessage request = toBatch.get(index);
				if (request.getReqType() == TOMMessageType.RECONFIG) {
					continue;
				}
				// if request with backward viewid, reply msg will include view content
				if (request.getViewID() < serverViewController.getCurrentViewId()) {

					View view = serverViewController.getCurrentView();

					List<NodeNetwork> addressesTemp = new ArrayList<>();

					for (int i = 0; i < view.getProcesses().length; i++) {
						int cpuId = view.getProcesses()[i];
						NodeNetwork inetSocketAddress = view.getAddress(cpuId);

						if (inetSocketAddress.getHost().equals("0.0.0.0")) {
							// proc docker env
							String host = serverViewController.getStaticConf().getOuterHostConfig().getHost(cpuId);

							NodeNetwork tempSocketAddress = new NodeNetwork(host, inetSocketAddress.getConsensusPort(),
									-1, inetSocketAddress.isConsensusSecure(), false);
							LOGGER.info("I am proc {}, tempSocketAddress.getAddress().getHostAddress() = {}",
									serverViewController.getStaticConf().getProcessId(), host);
							addressesTemp.add(tempSocketAddress);
						} else {
							LOGGER.info("I am proc {}, tempSocketAddress.getAddress().getHostAddress() = {}",
									serverViewController.getStaticConf().getProcessId(), inetSocketAddress.toString());
							addressesTemp.add(new NodeNetwork(inetSocketAddress.getHost(),
									inetSocketAddress.getConsensusPort(), -1, inetSocketAddress.isConsensusSecure(), false));
						}
					}

					View replyView = new View(view.getId(), view.getProcesses(), view.getF(),
							addressesTemp.toArray(new NodeNetwork[addressesTemp.size()]));
					LOGGER.info("I am proc {}, view = {}, reply View = {}",
							this.serverViewController.getStaticConf().getProcessId(), view, replyView);

					request.reply = new TOMMessage(id, request.getSession(), request.getSequence(),
							request.getOperationId(), asyncResponseLinkedList.get(index), TOMUtil.getBytes(replyView),
							serverViewController.getCurrentViewId(), request.getReqType());

				} else {
					request.reply = new TOMMessage(id, request.getSession(), request.getSequence(),
							request.getOperationId(), asyncResponseLinkedList.get(index), null,
							serverViewController.getCurrentViewId(), request.getReqType());
				}

				if (serverViewController.getStaticConf().getNumRepliers() > 0) {
					LOGGER.debug(
							"(ServiceReplica.receiveMessages) sending reply to {} with sequence number {} and operation ID {} via ReplyManager",
							request.getSender(), request.getSequence(), request.getOperationId());
					repMan.send(request);
				} else {
					LOGGER.debug(
							"(ServiceReplica.receiveMessages) sending reply to {} with sequence number {} and operation ID {}",
							request.getSender(), request.getSequence(), request.getOperationId());
					replier.manageReply(request, msgContexts[index]);
					// cs.send(new int[]{request.getSender()}, request.reply);
				}
			}

			// DEBUG
			LOGGER.debug("BATCHEXECUTOR END");
		} // End of: if (executor instanceof PreComputeBatchExecutable && numRequests > 0)
	}

	/**
	 * This method initializes the object
	 *
	 * @param cs   Server side communication System
	 * @param conf Total order messaging configuration
	 */
	private static ReplicaContext initTOMLayer(int currentProcessId, String realName, ServiceReplica replica,
                                               ServerCommunicationSystem cs, Recoverable recoverer, ServerViewController svc,
                                               RequestVerifier verifier, MessageHandler messageHandler,
                                               ClientCommunicationServerSide clientCommunication) {

		LOGGER.info("I am proc {}, init Tomlayer.", svc.getStaticConf().getProcessId());
		if (!svc.isInCurrentView()) {
			LOGGER.error(
					"I am proc {}, init Tomlayer, I am not in the specified view! --[ViewId={}][ViewProcessIds={}]",
					svc.getCurrentViewId(), Arrays.toString(svc.getCurrentViewProcesses()));
			throw new RuntimeException("I'm not an acceptor of the specified view!");
		}

		// Assemble the total order messaging layer
		MessageFactory messageFactory = new MessageFactory(currentProcessId);

		Acceptor acceptor = new Acceptor(cs, messageFactory, svc);

		messageHandler.setAcceptor(acceptor);

		Proposer proposer = new Proposer(cs, messageFactory, svc);

		ExecutionManager executionManager = new ExecutionManager(svc, acceptor, proposer, currentProcessId);

		acceptor.setExecutionManager(executionManager);

		TOMLayer tomLayer = new TOMLayer(executionManager, replica, recoverer, acceptor, cs, svc, verifier);
		tomLayer.setRealName(realName);
//		tomLayer.setLastExec(lastCid);
//		tomLayer.getStateManager().setLastCID(lastCid);

		executionManager.setTOMLayer(tomLayer);

		svc.setTomLayer(tomLayer);

		messageHandler.setTOMLayer(tomLayer);
		clientCommunication.setRequestReceiver(tomLayer);

		acceptor.setTOMLayer(tomLayer);

		if (svc.getStaticConf().isShutdownHookEnabled()) {
			Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(tomLayer));
		}
		LOGGER.info("I am proc {}, start Tomlayer!", currentProcessId);

		// TODO:
		tomLayer.start(); // start the layer execution

		return new ReplicaContext(tomLayer, svc);
	}

	/**
	 * Obtains the current replica context (getting access to several information
	 * and capabilities of the replication engine).
	 *
	 * @return this replica context
	 */
	public final ReplicaContext getReplicaContext() {
		return replicaCtx;
	}

	public ServerCommunicationSystem getServerCommunicationSystem() {
		return cs;
	}

	public void setCommunicationSystem(ServerCommunicationSystem cs) {
		this.cs = cs;
	}

	public int getId() {
		return id;
	}

	public TOMLayer getTomLayer() {
		return replicaCtx.getTOMLayer();
	}

	public String getRealmName() {
		return realmName;
	}
}
