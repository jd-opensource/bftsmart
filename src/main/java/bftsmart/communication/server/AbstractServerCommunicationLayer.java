package bftsmart.communication.server;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.communication.CommunicationException;
import bftsmart.communication.MacMessageCodec;
import bftsmart.communication.SystemMessage;
import bftsmart.communication.queue.MessageQueue;
import bftsmart.communication.queue.MessageQueue.SystemMessageType;
import bftsmart.communication.queue.MessageQueueFactory;
import bftsmart.reconfiguration.ViewTopology;

/**
 * @author huanghaiquan
 *
 */
public abstract class AbstractServerCommunicationLayer implements ServerCommunicationLayer {
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractServerCommunicationLayer.class);

//	private static final String PASSWORD = "commsyst";

	private Object connectionsLock = new Object();

	private Map<Integer, MessageConnection> connections = new ConcurrentHashMap<Integer, MessageConnection>();

	protected final int me;
	protected final String realmName;
	protected final ViewTopology topology;

	protected final MessageQueue messageInQueue;

	private final Map<SystemMessageType, AggregatedListeners> listeners = new ConcurrentHashMap<MessageQueue.SystemMessageType, AbstractServerCommunicationLayer.AggregatedListeners>();

	protected volatile boolean doWork = false;
//	protected SecretKey selfPwd;

	public AbstractServerCommunicationLayer(String realmName, ViewTopology topology) {
		this.topology = topology;
		this.messageInQueue = MessageQueueFactory.newMessageQueue(MessageQueue.QueueDirection.IN,
				topology.getStaticConf().getInQueueSize());
		this.me = topology.getCurrentProcessId();
		this.realmName = realmName;

//		selfPwd = initSelfKey();
	}

//	private SecretKey initSelfKey() {
//		try {
//			SecretKeyFactory fac = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
//			PBEKeySpec spec = new PBEKeySpec(PASSWORD.toCharArray());
//			return fac.generateSecret(spec);
//		} catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
//			throw new SecurityException(e.getMessage(), e);
//		}
//	}

	private void initConnections() {
		LOGGER.info("Start connecting to the other nodes of current view ... [CurrentProcessID={}][view={}]", me,
				Arrays.toString(topology.getCurrentViewProcesses()));
		int[] initialView = topology.getCurrentViewProcesses();
		MessageConnection[] conns = new MessageConnection[initialView.length];
		for (int i = 0; i < initialView.length; i++) {
			conns[i] = ensureConnection(initialView[i]);
		}
		for (MessageConnection conn : conns) {
			conn.start();
		}
	}

	@Override
	public int getId() {
		return me;
	}

	@Override
	public MacMessageCodec<SystemMessage> getMessageCodec(int id) {
		MessageConnection conn = connections.get(id);
		if (conn != null) {
			return conn.getMessageCodec();
		}
		return null;
	}

	public synchronized void updateConnections() {
		synchronized (connectionsLock) {
			if (this.topology.isInCurrentView()) {
				Integer[] remoteIds = this.connections.keySet().toArray(new Integer[this.connections.size()]);
				for (Integer remoteId : remoteIds) {
					if (!this.topology.isCurrentViewMember(remoteId)) {
						// 移除已经不属于当前视图的连接；
						MessageConnection conn = this.connections.remove(remoteId);
						conn.close();
					}
				}
			} else {
				// 当前节点已经不属于当前视图，关闭全部连接；
				for (MessageConnection conn : this.connections.values()) {
					conn.close();
				}
				this.connections.clear();

				close();
			}
		}
	}

	private MessageConnection getConnection(int remoteId) {
		MessageConnection ret = this.connections.get(remoteId);
		if (ret == null) {
			throw new IllegalStateException(
					String.format("Connection has not been established! --[Current=%s][Remote=%s]",
							topology.getCurrentProcessId(), remoteId));
		}
		return ret;
	}

	/**
	 * 确认到指定节点的连接；
	 * 
	 * <p>
	 * 
	 * 如果连接对象 {@link MessageConnection} 未建立，则初始化该连接；
	 * 
	 * @param remoteId
	 * @return
	 */
	private MessageConnection ensureConnection(int remoteId) {
		MessageConnection connection = null;
		connection = this.connections.get(remoteId);
		if (connection != null) {
			return connection;
		}
		synchronized (connectionsLock) {
			connection = this.connections.get(remoteId);
			if (connection != null) {
				return connection;
			}

			if (remoteId == me) {
				connection = connectLocal(me);
			} else if (isOutboundToRemote(remoteId)) {
				// 主动向外连接到远程节点；
				connection = connectRemote(remoteId);
			} else {
				// 等待外部节点接入；
				connection = acceptRemote(remoteId);
			}
			this.connections.put(remoteId, connection);

			LOGGER.debug("Ensure connection!  --[Current={}][Remote={}]", topology.getCurrentProcessId(), remoteId);
		}
		return connection;
	}

	/**
	 * 到指定远端节点的连接是否是应建立出站连接；
	 * 
	 * @param remoteId
	 * @return
	 */
	private boolean isOutboundToRemote(int remoteId) {
		boolean outbound = false;
		if (this.topology.isInCurrentView()) {
			// in this case, the node with higher ID starts the connection
			if (me > remoteId) {
				outbound = true;
			}
		}
		return outbound;
	}

	// ******* EDUARDO END **************//

	public void send(int[] targets, SystemMessage sm) {
		send(targets, sm, true);
	}

	@Override
	public final void send(int[] targets, SystemMessage sm, boolean retrySending) {
		if (!doWork) {
			throw new CommunicationException("ServerCommunicationLayer has stopped!");
		}

		@SuppressWarnings("unchecked")
		AsyncFuture<SystemMessage, Void>[] futures = new AsyncFuture[targets.length];
		int i = 0;
		for (int pid : targets) {
			try {
				// 对包括对当前节点的连接都统一抽象为 MessageConnection;
				futures[i] = ensureConnection(pid).send(sm, retrySending, new CompletedCallback<SystemMessage, Void>() {
					@Override
					public void onCompleted(SystemMessage source, Void result, Throwable error) {
						if (error != null) {
							LOGGER.error("Fail to send message[" + sm.getClass().getName() + "] to target proccess["
									+ pid + "]!");
						}
					}
				});
			} catch (Exception ex) {
				LOGGER.error("Failed to send messagea to target[" + pid + "]! --" + ex.getMessage(), ex);
			}

			i++;
		}
	}

	@Override
	public void addMessageListener(SystemMessageType type, MessageListener listener) {
		AggregatedListeners aggListeners = getListeners(type);
		aggListeners.addListener(listener);
	}

	private synchronized AggregatedListeners getListeners(SystemMessageType messageType) {
		AggregatedListeners aggListeners = listeners.get(messageType);
		if (aggListeners == null) {
			aggListeners = new AggregatedListeners();
			listeners.put(messageType, aggListeners);
		}
		return aggListeners;
	}

	/**
	 * 启动消息处理线程；
	 */
	private void startMessageProcessing() {
		for (SystemMessageType messageType : SystemMessageType.values()) {
			startProcess(messageType);
		}
	}

	private void startProcess(SystemMessageType messageType) {
		AggregatedListeners aggListeners = getListeners(messageType);
		Thread thrd = new Thread(new Runnable() {
			@Override
			public void run() {
				consume(messageType, aggListeners);
			}
		}, "MESSAGE-PROCESS-[" + messageType.toString() + "]");

		thrd.setDaemon(true);
		thrd.start();
	}

	private void consume(SystemMessageType messageType, MessageListener listener) {
		while (doWork) {
			SystemMessage message = null;
			try {
				message = messageInQueue.take(messageType);
			} catch (InterruptedException e) {
				continue;
			}
			if (message == null) {
				continue;
			}

			listener.onReceived(message);
		}

	}

	/**
	 * 停止消息处理线程；
	 */
	private void stopMessageProcessing() {
		// TODO Auto-generated method stub

	}

	@Override
	public synchronized void start() {
		if (doWork) {
			return;
		}

		if (!topology.isInCurrentView()) {
			String errMsg = String.format("Current node is beyond the view! --[CurrentId=%s][View=%s]",
					topology.getCurrentProcessId(), Arrays.toString(topology.getCurrentViewProcesses()));
			throw new IllegalStateException(errMsg);
		}

		doWork = true;

		initConnections();

		startCommunicationServer();

		startMessageProcessing();

	}

	public void close() {
		if (!doWork) {
			return;
		}
		LOGGER.info("Shutting down replica communication!");

		doWork = false;

		closeCommunicationServer();

		stopMessageProcessing();

		MessageConnection[] connections = this.connections.values()
				.toArray(new MessageConnection[this.connections.size()]);
		this.connections.clear();
		for (MessageConnection conn : connections) {
			conn.close();
		}
	}

	/**
	 * 启动通讯服务器；
	 */
	protected abstract void startCommunicationServer();

	/**
	 * 启动通讯服务器；
	 */
	protected abstract void closeCommunicationServer();

	/**
	 * 创建与当前字节自身的连接；
	 * <p>
	 * 
	 * 默认实现：基于本地队列进行消息直接投递；
	 * 
	 * @param currentId
	 * @return
	 */
	protected MessageConnection connectLocal(int currentId) {
		return new LoopbackConnection(realmName, currentId, messageInQueue);
	}

	/**
	 * 尝试连接远端节点；
	 * <p>
	 * 
	 * 实现者应提供异步的实现，确保实施以下约束：
	 * <p>
	 * 1.无论是否已实际连接成功，此方法都会立即返回一个 {@link MessageConnection} 实例，不会堵塞当前操作； <br>
	 * 
	 * 2. 通过 {@link MessageConnection#isAlived()} 可以判断连接是否已经就绪； <br>
	 * 
	 * 3. 调用未就绪的 {@link MessageConnection} 实例发送时将缓存消息，直到连接建立之后再发送； <br>
	 * 
	 * @param remoteId
	 * @return
	 */
	protected abstract MessageConnection connectRemote(int remoteId);

	/**
	 * 开始接受从指定节点的接入；
	 * <p>
	 * 
	 * 实现者应提供异步的实现，确保实施以下约束：
	 * <p>
	 * 1.无论是否已实际接入，此方法都会立即返回一个 {@link MessageConnection} 实例，不会堵塞当前操作； <br>
	 * 
	 * 2. 通过 {@link MessageConnection#isAlived()} 可以判断连接是否已经就绪； <br>
	 * 
	 * 3. 调用未就绪的 {@link MessageConnection} 实例发送时将缓存消息，直到连接建立之后再发送； <br>
	 * 
	 * @param remoteId
	 * @return
	 */
	protected abstract MessageConnection acceptRemote(int remoteId);

	@Override
	public String toString() {
		String str = "inQueue=" + messageInQueue.toString();

		int[] activeServers = topology.getCurrentViewProcesses();

		for (int i = 0; i < activeServers.length; i++) {

			// for(int i=0; i<connections.length; i++) {
			// if(connections[i] != null) {
			if (me != activeServers[i]) {
				str += ", connections[" + activeServers[i] + "]: server-connection=" + getConnection(activeServers[i]);
			}
		}

		return str;
	}

	private static class AggregatedListeners implements MessageListener {

		private volatile MessageListener[] listeners = new MessageListener[0];

		private synchronized void addListener(MessageListener listener) {
			MessageListener[] newListeners = new MessageListener[this.listeners.length + 1];
			System.arraycopy(this.listeners, 0, newListeners, 0, this.listeners.length);
			newListeners[this.listeners.length] = listener;
			this.listeners = newListeners;
		}

		@Override
		public void onReceived(SystemMessage message) {
			MessageListener[] messageListeners = this.listeners;
			for (MessageListener messageListener : messageListeners) {
				messageListener.onReceived(message);
			}
		}

	}
}
