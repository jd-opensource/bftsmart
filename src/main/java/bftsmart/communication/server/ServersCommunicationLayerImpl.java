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
package bftsmart.communication.server;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.communication.SystemMessage;
import bftsmart.communication.queue.MessageQueue;
import bftsmart.reconfiguration.ReplicaTopology;
import bftsmart.reconfiguration.ViewTopology;
import bftsmart.tom.ServiceReplica;

/**
 * @author huanghaiquan
 *
 */
public class ServersCommunicationLayerImpl implements ServersCommunicationLayer {
	private static final Logger LOGGER = LoggerFactory.getLogger(ServersCommunicationLayerImpl.class);

	private ReplicaTopology topology;

	private Object connectionsLock = new Object();
	
	private Map<Integer, MessageConnection> connections = new ConcurrentHashMap<Integer, MessageConnection>();
	private volatile ServerSocket serverSocket;
	private int me;
	private volatile boolean doWork = true;
	
	private ServiceReplica replica;
	
	private SecretKey selfPwd;
	private MessageQueue messageInQueue;
	private static final String PASSWORD = "commsyst";

	public ServersCommunicationLayerImpl(ReplicaTopology topology, MessageQueue messageInQueue, ServiceReplica replica)
			throws Exception {
		this.topology = topology;
		this.messageInQueue = messageInQueue;
		this.me = topology.getCurrentProcessId();
		this.replica = replica;

		SecretKeyFactory fac = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
		PBEKeySpec spec = new PBEKeySpec(PASSWORD.toCharArray());
		selfPwd = fac.generateSecret(spec);

		initConnections();

		initServerSocket(topology);
	}

	private void initConnections() {
		// Try connecting if a member of the current view. Otherwise, wait until the
		// Join has been processed!
		if (topology.isInCurrentView()) {
			LOGGER.info("Start connecting to the other nodes of current view[{}]...[CurrentProcessID={}]",
					topology.getCurrentView().getId(), topology.getStaticConf().getProcessId());
			int[] initialV = topology.getCurrentViewAcceptors();
			for (int i = 0; i < initialV.length; i++) {
				if (initialV[i] != me) {
					ensureConnection(initialV[i]);
				}
			}
		}

		connections.put(me, new SelfConnection(replica.getRealName(), topology, messageInQueue));
	}

	private void initServerSocket(ViewTopology controller) throws IOException, SocketException {
		serverSocket = new ServerSocket(
				controller.getStaticConf().getServerToServerPort(controller.getStaticConf().getProcessId()));
		serverSocket.setSoTimeout(10000);
		serverSocket.setReuseAddress(true);
	}

	public SecretKey getSecretKey(int id) {
		if (id == topology.getStaticConf().getProcessId())
			return selfPwd;
		else if (connections.get(id) != null) {
			return connections.get(id).getSecretKey();
		}

		return null;
	}

	// ******* EDUARDO BEGIN **************//
	public synchronized void updateConnections() {
		synchronized (connectionsLock) {
			if (this.topology.isInCurrentView()) {

				Integer[] remoteIds = this.connections.keySet().toArray(new Integer[this.connections.size()]);
				for (Integer remoteId : remoteIds) {
					if (!this.topology.isCurrentViewMember(remoteId)) {
						MessageConnection conn = this.connections.remove(remoteId);
						conn.shutdown();
					}
				}

				int[] newV = topology.getCurrentViewAcceptors();
				for (int i = 0; i < newV.length; i++) {
					if (newV[i] != me) {
						ensureConnection(newV[i]);
					}
				}
			} else {

				Iterator<Integer> it = this.connections.keySet().iterator();
				while (it.hasNext()) {
					this.connections.get(it.next()).shutdown();
				}
			}
		}
	}

	public void resetConnection(int remoteId) {
		MessageConnection conn = this.connections.remove(remoteId);
		if (conn != null) {
			conn.shutdown();
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

	private MessageConnection ensureConnection(int remoteId) {
		MessageConnection connection = null;
		connection = this.connections.get(remoteId);
		if (connection == null) {
			synchronized (connectionsLock) {
				connection = this.connections.get(remoteId);
				if (connection == null) {
					if (isOutgoingToRemote(remoteId)) {
						OutgoingSockectConnection conn = new OutgoingSockectConnection(this.replica.getRealName(),
								topology, remoteId, messageInQueue);
						this.connections.put(remoteId, conn);
						connection = conn;
					} else {
						IncomingSockectConnection conn = new IncomingSockectConnection(this.replica.getRealName(),
								topology, remoteId, this.messageInQueue);
						
						
						this.connections.put(remoteId, conn);
						connection = conn;
					}
					LOGGER.debug("Ensure connection!  --[Current={}][Remote={}]", topology.getCurrentProcessId(),
							remoteId);
				}
			}
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

	public void send(int[] targets, SystemMessage sm, boolean useMAC, boolean retrySending) {
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

//				if (pid == me) {
//					sm.authenticated = true;
//					MessageQueue.SystemMessageType msgType = MessageQueue.SystemMessageType.typeOf(sm);
//					messageInQueue.put(msgType, sm);
//				} else {
//					// System.out.println("Going to send message to: "+i);
//					// ******* EDUARDO BEGIN **************//
//					// connections[i].send(data);
////                    LOGGER.info("I am {}, send data to {}, which is {} !", controller.getStaticConf().getProcessId(), i, sm.getClass());
//					futures[i] = ensureConnection(pid).send(sm, useMAC, retrySending,
//							new CompletedCallback<SystemMessage, Void>() {
//								@Override
//								public void onCompleted(SystemMessage source, Void result, Throwable error) {
//									if (error != null) {
//										LOGGER.error("Fail to send message[" + sm.getClass().getName()
//												+ "] to target proccess[" + pid + "]!");
//									}
//								}
//							});
//
//					// ******* EDUARDO END **************//
//				}
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

	public void shutdown() {
		if (!doWork) {
			return;
		}
		LOGGER.info("Shutting down replica sockets");

		doWork = false;

		// ******* EDUARDO BEGIN **************//
		MessageConnection[] connections = this.connections.values()
				.toArray(new ServerSockectConnection[this.connections.size()]);
		this.connections.clear();
		for (MessageConnection serverConnection : connections) {
			serverConnection.shutdown();
		}
		try {
			ServerSocket sc = serverSocket;
			serverSocket = null;
			if (sc != null) {
				sc.close();
			}
		} catch (Exception e) {
			LOGGER.warn("Error occurred while closing server socket! --[" + this.topology.getStaticConf().getProcessId()
					+ "] " + e.getMessage(), e);
		}
	}

//	public void joinViewReceived() {
//		waitViewLock.lock();
//		for (int i = 0; i < pendingConn.size(); i++) {
//			PendingConnection pc = pendingConn.get(i);
//			try {
//				establishConnection(pc.s, pc.remoteId);
//			} catch (Exception e) {
//				LOGGER.warn("Error occurred while establishing connection! --" + e.getMessage(), e);
//			}
//		}
//
//		pendingConn.clear();
//
//		waitViewLock.unlock();
//	}

	@Override
	public void startListening() {
		Thread thrd = new Thread(new Runnable() {
			@Override
			public void run() {
				acceptConnection();
			}
		}, "Servers Connection Listener");
		thrd.setDaemon(true);
		thrd.start();
	}

	private void acceptConnection() {
		while (doWork) {
			try {

				// System.out.println("Waiting for server connections");

				Socket newSocket = serverSocket.accept();

				SocketUtils.setSocketOptions(newSocket);
				int remoteId = new DataInputStream(newSocket.getInputStream()).readInt();

				LOGGER.info("I am {} establishConnection run!", this.topology.getStaticConf().getProcessId());
				establishConnection(newSocket, remoteId);
				
				// ******* EDUARDO BEGIN **************//
////				if (!this.topology.isInCurrentView() && (this.topology.getStaticConf().getTTPId() != remoteId)) {
//				if (!this.topology.isInCurrentView()) {
//					waitViewLock.lock();
//					pendingConn.add(new PendingConnection(newSocket, remoteId));
//					waitViewLock.unlock();
//				} else {
//					LOGGER.info("I am {} establishConnection run!", this.topology.getStaticConf().getProcessId());
//					establishConnection(newSocket, remoteId);
//				}
				// ******* EDUARDO END **************//

			} catch (SocketTimeoutException ex) {
				// timeout on the accept... do nothing
			} catch (SocketException ex) {
				if (doWork) {
					LOGGER.error("Socket error occurred while accepting incoming connection! --[CurrentProcessId="
							+ this.topology.getStaticConf().getProcessId() + "]" + ex.getMessage(), ex);
					try {
						serverSocket.close();
					} catch (Exception e) {
					}
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e1) {
					}
					try {
						initServerSocket(topology);
					} catch (Exception e) {
						LOGGER.error("Retry to create server socket fail! --[CurrentProcessId="
								+ this.topology.getStaticConf().getProcessId() + "]" + ex.getMessage(), ex);
					}
				}
			} catch (Exception ex) {
				if (doWork) {
					LOGGER.error("Unexpected error occurred while accepting incoming connection! --[CurrentProcessId="
							+ this.topology.getStaticConf().getProcessId() + "]" + ex.getMessage(), ex);
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e1) {
					}
				}
			}
		}

		try {
			serverSocket.close();
		} catch (Throwable e) {
			// other exception or error
			LOGGER.warn("Error occurred while closing the server socket of current node! --" + e.getMessage(), e);
		}

		LOGGER.info("ServerCommunicationLayer stopped! --[" + this.topology.getStaticConf().getProcessId() + "]");
	}

	// ******* EDUARDO BEGIN **************//
	private void establishConnection(Socket newSocket, int remoteId) throws IOException {
		if (!this.topology.isCurrentViewMember(remoteId)) {
			LOGGER.warn(
					"The incoming socket will be aborted because it is from a remote node beyond the current view! --[RemoteId={}][CurrentId={}]",
					remoteId, me);
			newSocket.close();
			return;
		}
		synchronized (connectionsLock) {
			MessageConnection conn = this.connections.get(remoteId);
			if (conn == null) { // This must never happen!!!
				// first time that this connection is being established
				IncomingSockectConnection incomingConnection = new IncomingSockectConnection(replica.getRealName(),
						topology, remoteId, messageInQueue);
				incomingConnection.acceptSocket(newSocket);
				this.connections.put(remoteId, incomingConnection);
				conn = incomingConnection;
			} else {
				// reconnection
				if (!(conn instanceof IncomingSockectConnection)) {
					// illegal connection types;
					LOGGER.error(
							"Wrong connection type to accept new incoming socket! --[ExpectedConnectionType={}][RemoteId={}][CurrentId={}]",
							conn.getClass().getName(), remoteId, me);
					newSocket.close();
					return;
				}
				IncomingSockectConnection incomingConnection = (IncomingSockectConnection) conn;
				if (incomingConnection.isAlived()) {
					// don't interrupt aliving connection;
					LOGGER.warn(
							"Abort the new incoming socket because an aliving connection from the same remote already exist! --[ExpectedConnectionType={}][RemoteId={}][CurrentId={}]",
							conn.getClass().getName(), remoteId, me);
					newSocket.close();
					return;
				}
				incomingConnection.acceptSocket(newSocket);
			}
		}
	}
	// ******* EDUARDO END **************//

	@Override
	public String toString() {
		String str = "inQueue=" + messageInQueue.toString();

		int[] activeServers = topology.getCurrentViewAcceptors();

		for (int i = 0; i < activeServers.length; i++) {

			// for(int i=0; i<connections.length; i++) {
			// if(connections[i] != null) {
			if (me != activeServers[i]) {
				str += ", connections[" + activeServers[i] + "]: server-connection=" + getConnection(activeServers[i]);
			}
		}

		return str;
	}

	// ******* EDUARDO BEGIN: List entry that stores pending connections,
	// as a server may accept connections only after learning the current view,
	// i.e., after receiving the response to the join*************//
	// This is for avoiding that the server accepts connectsion from everywhere
	public class PendingConnection {

		public Socket s;
		public int remoteId;

		public PendingConnection(Socket s, int remoteId) {
			this.s = s;
			this.remoteId = remoteId;
		}
	}

	// ******* EDUARDO END **************//
}
