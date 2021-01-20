package bftsmart.communication.server;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.communication.SystemMessage;
import bftsmart.communication.queue.MessageQueue;
import bftsmart.communication.queue.MessageQueue.SystemMessageType;
import bftsmart.reconfiguration.ViewTopology;

/**
 * @author huanghaiquan
 *
 */
public abstract class AbstractServersCommunicationLayer implements ServerCommunicationLayer {
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractServersCommunicationLayer.class);

	private static final String PASSWORD = "commsyst";

	private Object connectionsLock = new Object();

	private Map<Integer, MessageConnection> connections = new ConcurrentHashMap<Integer, MessageConnection>();

	protected final int me;
	protected final String realmName;
	protected final ViewTopology topology;

	protected final MessageQueue messageInQueue;

	protected volatile boolean doWork = false;
	protected SecretKey selfPwd;

	public AbstractServersCommunicationLayer(String realmName, ViewTopology topology, MessageQueue messageInQueue) {
		this.topology = topology;
		this.messageInQueue = messageInQueue;
		this.me = topology.getCurrentProcessId();
		this.realmName = realmName;

		selfPwd = initSelfKey();
	}

	private SecretKey initSelfKey() {
		try {
			SecretKeyFactory fac = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
			PBEKeySpec spec = new PBEKeySpec(PASSWORD.toCharArray());
			return fac.generateSecret(spec);
		} catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
			throw new SecurityException(e.getMessage(), e);
		}
	}

	private void initConnections() {
		LOGGER.info("Start connecting to the other nodes of current view[{}]...[CurrentProcessID={}]",
				topology.getCurrentView().getId(), me);
		int[] initialV = topology.getCurrentViewProcesses();
		for (int i = 0; i < initialV.length; i++) {
			ensureConnection(initialV[i]);
		}
	}

	public SecretKey getSecretKey(int id) {
		if (id == me)
			return selfPwd;
		else if (connections.get(id) != null) {
			return connections.get(id).getSecretKey();
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
						conn.shutdown();
					}
				}
//				int[] newV = topology.getCurrentViewProcesses();
//				for (int i = 0; i < newV.length; i++) {
//					if (newV[i] != me) {
//						//加入新节点的连接；
//						ensureConnection(newV[i]);
//					}
//				}
			} else {
				// 当前节点已经不属于当前视图，关闭全部连接；
				for (MessageConnection conn : this.connections.values()) {
					conn.shutdown();
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
				connection = new MessageQueueConnection(realmName, remoteId, messageInQueue);
			} else if (isOutgoingToRemote(remoteId)) {
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
	 * 到指定节点是否是应建立外向连接；
	 * 
	 * @param remoteId
	 * @return
	 */
	private boolean isOutgoingToRemote(int remoteId) {
		boolean ret = false;
		if (this.topology.isInCurrentView()) {
			// in this case, the node with higher ID starts the connection
			if (me > remoteId) {
				ret = true;
			}
		}
		return ret;
	}

	// ******* EDUARDO END **************//

	public void send(int[] targets, SystemMessage sm, boolean useMAC) {
		send(targets, sm, useMAC, true);
	}

	public final void send(int[] targets, SystemMessage sm, boolean useMAC, boolean retrySending) {
		@SuppressWarnings("unchecked")
		AsyncFuture<SystemMessage, Void>[] futures = new AsyncFuture[targets.length];
		int i = 0;
		for (int pid : targets) {
			try {
				// 对包括对当前节点的连接都统一抽象为 MessageConnection;
				futures[i] = ensureConnection(pid).send(sm, useMAC, retrySending,
						new CompletedCallback<SystemMessage, Void>() {
							@Override
							public void onCompleted(SystemMessage source, Void result, Throwable error) {
								if (error != null) {
									LOGGER.error("Fail to send message[" + sm.getClass().getName()
											+ "] to target proccess[" + pid + "]!");
								}
							}
						});
			} catch (Exception ex) {
				LOGGER.error("Failed to send messagea to target[" + pid + "]! --" + ex.getMessage(), ex);
			}

			i++;
		}

		// 检查发送成功的数量；
//		for (int j = 0; j < futures.length; j++) {
//			//阻塞等待返回；
//			futures[i].getReturn(1000);
//		}
	}
	
	@Override
	public SystemMessage consume(SystemMessageType type, long timeout, TimeUnit unit) {
		try {
			return messageInQueue.poll(type, timeout, unit);
		} catch (InterruptedException e) {
			return null;
		}
	}

	public void close() {
		if (!doWork) {
			return;
		}
		LOGGER.info("Shutting down replica communication!");

		doWork = false;

		closeCommunicationServer();

		MessageConnection[] connections = this.connections.values()
				.toArray(new MessageConnection[this.connections.size()]);
		this.connections.clear();
		for (MessageConnection conn : connections) {
			conn.shutdown();
		}
	}

	@Override
	public synchronized void start() {
//		Thread thrd = new Thread(new Runnable() {
//			@Override
//			public void run() {
//				acceptConnection();
//			}
//		}, "Servers Connection Listener");
//		thrd.setDaemon(true);
//		thrd.start();

		if (doWork) {
			return;
		}

		if (!topology.isInCurrentView()) {
			throw new IllegalStateException("Current node is beyond the view!");
		}

		doWork = true;

		initConnections();

		startCommunicationServer();
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
}
