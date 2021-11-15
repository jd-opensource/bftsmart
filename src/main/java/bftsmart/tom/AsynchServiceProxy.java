package bftsmart.tom;

import bftsmart.communication.client.ReplyListener;
import bftsmart.reconfiguration.util.TOMConfiguration;
import bftsmart.reconfiguration.views.View;
import bftsmart.reconfiguration.views.ViewStorage;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.util.Extractor;
import bftsmart.tom.util.TOMUtil;
import org.slf4j.LoggerFactory;
import utils.net.SSLSecurity;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is an extension of 'ServiceProxy' that can waits for replies
 * asynchronously.
 *
 * @author Andre Nogueira
 *
 */
public class AsynchServiceProxy extends ServiceProxy {

	/**
	 *
	 */
	private Map<Integer, RequestContext> requestsContext;
	private Map<Integer, TOMMessage[]> requestsReplies;
	private Map<Integer, Integer> requestsAlias;
	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(AsynchServiceProxy.class);
	
//	/**
//	 *
//	 * @param processId
//	 *            Replica id
//	 */
//	public AsynchServiceProxy(int processId) {
//		this(processId, "config/system.config", "config/hosts.config", "config", null, null);
//		// init();
//	}
//
//	/**
//	 *
//	 * @param processId
//	 *            Replica id
//	 * @param configHome
//	 *            Configuration folder
//	 */
//	public AsynchServiceProxy(int processId, String configHome) {
//		this(processId, configHome + "/system.config", configHome + "/hosts.config", configHome, null, null);
//		init();
//	}

	/**
	 *
	 * @param processId
	 *            Replica id
	 * @param configHome
	 *            Configuration folder
	 */
	public AsynchServiceProxy(TOMConfiguration config, ViewStorage viewStorage) {
		this(config, viewStorage, new SSLSecurity());
	}

	public AsynchServiceProxy(TOMConfiguration config, ViewStorage viewStorage, SSLSecurity sslSecurity) {
		super(config, viewStorage, null, null, sslSecurity);
		init();
	}

//	public AsynchServiceProxy(int processId, String systemConfigFile, String hostsConfigFile, String keystoreHome, Comparator<byte[]> replyComparator,
//			Extractor replyExtractor) {
//		super(processId, systemConfigFile, hostsConfigFile,  keystoreHome, replyComparator, replyExtractor);
//		init();
//	}

	private void init() {
		requestsContext = new ConcurrentHashMap<>();
		requestsReplies = new ConcurrentHashMap<>();
		requestsAlias = new ConcurrentHashMap<>();
	}

	private View newView(byte[] bytes) {

		Object o = TOMUtil.getObject(bytes);
		return (o != null && o instanceof View ? (View) o : null);
	}

	/**
	 *
	 * @param request
	 * @param replyListener
	 * @param reqType
	 *            Request type
	 * @return
	 */
	public int invokeAsynchRequest(byte[] request, ReplyListener replyListener, TOMMessageType reqType) {
		return invokeAsynchRequest(request, super.getViewManager().getCurrentViewProcesses(), replyListener, reqType);
	}

	/**
	 *
	 * @param request
	 * @param targets
	 * @param replyListener
	 * @param reqType
	 *            Request type
	 * @return
	 */
	public int invokeAsynchRequest(byte[] request, int[] targets, ReplyListener replyListener, TOMMessageType reqType) {
		return invokeAsynch(request, targets, replyListener, reqType);
	}

	/**
	 *
	 * @param requestId
	 *            Request
	 */
	public void cleanAsynchRequest(int requestId) {

		Integer id = requestId;

		do {

			requestsContext.remove(id);
			requestsReplies.remove(id);

			id = requestsAlias.remove(id);

		} while (id != null);

	}

	/**
	 *
	 */
	@Override
	public void replyReceived(TOMMessage reply) {
		LOGGER.debug("Asynchronously received reply from {} with sequence number {} and operation ID {}", reply.getSender(), reply.getSequence(), reply.getOperationId());

		canReceiveLock.lock();
		try {
			RequestContext requestContext = requestsContext.get(reply.getOperationId());

			if (requestContext == null) { // it is not a asynchronous request
				super.replyReceived(reply);
				return;
			}

			if (contains(requestContext.getTargets(), reply.getSender())
					&& (reply.getSequence() == requestContext.getReqId())
					// && (reply.getOperationId() == requestContext.getOperationId())
					&& (reply.getReqType().compareTo(requestContext.getRequestType())) == 0) {

				LOGGER.debug("Deliverying message from {} with sequence number {} and operation ID {} to the listener", reply.getSender(), reply.getSequence(), reply.getOperationId());

				ReplyListener replyListener = requestContext.getReplyListener();

				View v = null;

				if (replyListener != null) {

					// if (reply.getViewID() > getViewManager().getCurrentViewId()) { // Deal with a
					// system reconfiguration
					if ((v = newView(reply.getContent())) != null
							&& !requestsAlias.containsKey(reply.getOperationId())) { // Deal with a system
																						// reconfiguration

						TOMMessage[] replies = requestsReplies.get(reply.getOperationId());

						int sameContent = 1;
						int replyQuorum = getReplyQuorum();

						int pos = getViewManager().getCurrentViewPos(reply.getSender());

						replies[pos] = reply;

						for (int i = 0; i < replies.length; i++) {

							if ((replies[i] != null) && (i != pos || getViewManager().getCurrentViewN() == 1)
									&& (reply.getReqType() != TOMMessageType.ORDERED_REQUEST
											|| Arrays.equals(replies[i].getContent(), reply.getContent()))) {
								sameContent++;
							}
						}

						if (sameContent >= replyQuorum) {

							if (v.getId() > getViewManager().getCurrentViewId()) {

								reconfigureTo(v);
							}

							requestContext.getReplyListener().reset();

							Thread t = new Thread() {

								@Override
								public void run() {

									int id = invokeAsynch(requestContext.getRequest(), requestContext.getTargets(),
											requestContext.getReplyListener(), TOMMessageType.ORDERED_REQUEST);

									requestsAlias.put(reply.getOperationId(), id);
								}

							};

							t.start();

						}

					} else if (!requestsAlias.containsKey(reply.getOperationId())) {

						requestContext.getReplyListener().replyReceived(requestContext, reply);
					}
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			canReceiveLock.unlock();
		}
	}

	/**
	 *
	 * @param request
	 * @param targets
	 * @param replyListener
	 * @param reqType
	 * @return
	 */
	private int invokeAsynch(byte[] request, int[] targets, ReplyListener replyListener, TOMMessageType reqType) {

		LOGGER.debug("Asynchronously sending request to {}", Arrays.toString(targets));

		RequestContext requestContext = null;

		canSendLock.lock();

		requestContext = new RequestContext(generateRequestId(reqType), generateOperationId(), reqType, targets,
				System.currentTimeMillis(), replyListener, request);

		try {
			LOGGER.debug("Storing request context for {}", requestContext.getOperationId());
			requestsContext.put(requestContext.getOperationId(), requestContext);
			requestsReplies.put(requestContext.getOperationId(),
					new TOMMessage[super.getViewManager().getCurrentViewN()]);

			sendMessageToTargets(request, requestContext.getReqId(), requestContext.getOperationId(), targets, reqType);

		} finally {
			canSendLock.unlock();
		}

		return requestContext.getOperationId();
	}

	/**
	 *
	 * @param targets
	 * @param senderId
	 * @return
	 */
	private boolean contains(int[] targets, int senderId) {
		for (int i = 0; i < targets.length; i++) {
			if (targets[i] == senderId) {
				return true;
			}
		}
		return false;
	}

}
