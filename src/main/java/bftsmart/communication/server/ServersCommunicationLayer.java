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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jd.blockchain.utils.io.RuntimeIOException;

import bftsmart.communication.SystemMessage;
import bftsmart.communication.queue.MessageQueue;
import bftsmart.communication.queue.MessageQueueFactory;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.tom.ServiceReplica;

/**
 *
 * @author alysson
 */
public class ServersCommunicationLayer extends Thread {
	private static final Logger LOGGER = LoggerFactory.getLogger(ServersCommunicationLayer.class);

	private ServerViewController controller;
//    private LinkedBlockingQueue<SystemMessage> inQueue;
	private Map<Integer, ServerConnection> connections = new ConcurrentHashMap<Integer, ServerConnection>();
	private volatile ServerSocket serverSocket;
	private int me;
	private boolean doWork = true;
	private Lock connectionsLock = new ReentrantLock();
	private ReentrantLock waitViewLock = new ReentrantLock();
	// private Condition canConnect = waitViewLock.newCondition();
	private List<PendingConnection> pendingConn = Collections.synchronizedList(new LinkedList<PendingConnection>());
	private ServiceReplica replica;
	private SecretKey selfPwd;
	private MessageQueue messageInQueue;
	private static final String PASSWORD = "commsyst";

	public ServersCommunicationLayer(ServerViewController controller, MessageQueue messageInQueue,
			ServiceReplica replica) throws Exception {

		this.controller = controller;
		this.messageInQueue = messageInQueue;
		this.me = controller.getStaticConf().getProcessId();
		this.replica = replica;

		// Try connecting if a member of the current view. Otherwise, wait until the
		// Join has been processed!
		if (controller.isInCurrentView()) {
			LOGGER.info("Start connecting to the other nodes of current view[{}]...[CurrentProcessID={}]",
					controller.getCurrentView().getId(), controller.getStaticConf().getProcessId());
			int[] initialV = controller.getCurrentViewAcceptors();
			for (int i = 0; i < initialV.length; i++) {
				if (initialV[i] != me) {
					getConnection(initialV[i]);
				}
			}
		}

		SecretKeyFactory fac = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
		PBEKeySpec spec = new PBEKeySpec(PASSWORD.toCharArray());
		selfPwd = fac.generateSecret(spec);

		createServerSocket(controller);

		start();
	}

	private void createServerSocket(ServerViewController controller) throws IOException, SocketException {
		serverSocket = new ServerSocket(
				controller.getStaticConf().getServerToServerPort(controller.getStaticConf().getProcessId()));
		serverSocket.setSoTimeout(10000);
		serverSocket.setReuseAddress(true);
	}

//    public ServersCommunicationLayer(ServerViewController controller,
//                                     LinkedBlockingQueue<SystemMessage> inQueue, ServiceReplica replica) throws Exception {
//
//        this.controller = controller;
//        this.inQueue = inQueue;
//        this.me = controller.getStaticConf().getProcessId();
//        this.replica = replica;
//
//        //Try connecting if a member of the current view. Otherwise, wait until the Join has been processed!
//        if (controller.isInCurrentView()) {
//            int[] initialV = controller.getCurrentViewAcceptors();
//            for (int i = 0; i < initialV.length; i++) {
//                if (initialV[i] != me) {
//                    getConnection(initialV[i]);
//                }
//            }
//        }
//
//        serverSocket = new ServerSocket(controller.getStaticConf().getServerToServerPort(
//                controller.getStaticConf().getProcessId()));
//
//        SecretKeyFactory fac = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
//        PBEKeySpec spec = new PBEKeySpec(PASSWORD.toCharArray());
//        selfPwd = fac.generateSecret(spec);
//
//        serverSocket.setSoTimeout(10000);
//        serverSocket.setReuseAddress(true);
//
//        start();
//    }

	public SecretKey getSecretKey(int id) {
		if (id == controller.getStaticConf().getProcessId())
			return selfPwd;
		else if (connections.get(id) != null) {
			return connections.get(id).getSecretKey();
		}

		return null;
	}

	// ******* EDUARDO BEGIN **************//
	public synchronized void updateConnections() {
		connectionsLock.lock();

		if (this.controller.isInCurrentView()) {

			Iterator<Integer> it = this.connections.keySet().iterator();
			List<Integer> toRemove = new LinkedList<Integer>();
			while (it.hasNext()) {
				int rm = it.next();
				if (!this.controller.isCurrentViewMember(rm)) {
					toRemove.add(rm);
				}
			}
			for (int i = 0; i < toRemove.size(); i++) {
				this.connections.remove(toRemove.get(i)).shutdown();
			}

			int[] newV = controller.getCurrentViewAcceptors();
			for (int i = 0; i < newV.length; i++) {
				if (newV[i] != me) {
					getConnection(newV[i]);
				}
			}
		} else {

			Iterator<Integer> it = this.connections.keySet().iterator();
			while (it.hasNext()) {
				this.connections.get(it.next()).shutdown();
			}
		}

		connectionsLock.unlock();
	}

	public synchronized ServerConnection updateConnection(int remoteId) {
		ServerConnection conn = this.connections.remove(remoteId);
		if (conn != null) {
			conn.shutdown();
		}
		return conn;
	}

	public synchronized ServerConnection getConnection(int remoteId) {
		connectionsLock.lock();
		ServerConnection ret = this.connections.get(remoteId);
		if (ret == null) {
			LOGGER.info("I am {}, remote = {} !", controller.getStaticConf().getProcessId(), remoteId);
			ret = new ServerConnection(controller, null, remoteId, this.messageInQueue, this.replica);
			this.connections.put(remoteId, ret);
		}
		connectionsLock.unlock();
		return ret;
	}
	// ******* EDUARDO END **************//

	public void send(int[] targets, SystemMessage sm, boolean useMAC) {
		send(targets, sm, useMAC, true);
	}

	public void send(int[] targets, SystemMessage sm, boolean useMAC, boolean retrySending) {
		// 首先判断消息类型
		ByteArrayOutputStream bOut = new ByteArrayOutputStream(248);
		try {
			new ObjectOutputStream(bOut).writeObject(sm);
		} catch (IOException ex) {
			throw new RuntimeIOException(ex.getMessage(), ex);
		}

		byte[] data = bOut.toByteArray();
		@SuppressWarnings("unchecked")
		AsyncFuture<byte[], Void>[] futures = new AsyncFuture[targets.length];
		int i = 0;
		for (int pid : targets) {
			try {
				if (pid == me) {
					sm.authenticated = true;
					MessageQueue.MSG_TYPE msgType = MessageQueueFactory.msgType(sm);
					messageInQueue.put(msgType, sm);
				} else {
					// System.out.println("Going to send message to: "+i);
					// ******* EDUARDO BEGIN **************//
					// connections[i].send(data);
//                    LOGGER.info("I am {}, send data to {}, which is {} !", controller.getStaticConf().getProcessId(), i, sm.getClass());
					futures[i] = getConnection(pid).send(data, useMAC, retrySending,
							new CompletedCallback<byte[], Void>() {
								@Override
								public void onCompleted(byte[] source, Void result, Throwable error) {
									if (error != null) {
										LOGGER.error("Fail to send message[" + sm.getClass().getName()
												+ "] to target proccess[" + pid + "]!");
									}
								}
							});

					// ******* EDUARDO END **************//
				}
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

		LOGGER.info("Shutting down replica sockets");

		doWork = false;

		// ******* EDUARDO BEGIN **************//
		int[] activeServers = controller.getCurrentViewAcceptors();

		for (int i = 0; i < activeServers.length; i++) {
			// if (connections[i] != null) {
			// connections[i].shutdown();
			// }
			if (me != activeServers[i]) {
				getConnection(activeServers[i]).shutdown();
			}
		}
		try {
			serverSocket.close();
		} catch (Exception e) {
			LOGGER.warn("Error occurred while closing server socket! --["
					+ this.controller.getStaticConf().getProcessId() + "] " + e.getMessage(), e);
		}
	}

	// ******* EDUARDO BEGIN **************//
	public void joinViewReceived() {
		waitViewLock.lock();
		for (int i = 0; i < pendingConn.size(); i++) {
			PendingConnection pc = pendingConn.get(i);
			try {
				establishConnection(pc.s, pc.remoteId);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		pendingConn.clear();

		waitViewLock.unlock();
	}
	// ******* EDUARDO END **************//

	@Override
	public void run() {
		while (doWork) {
			try {

				// System.out.println("Waiting for server connections");

				Socket newSocket = serverSocket.accept();

				ServersCommunicationLayer.setSocketOptions(newSocket);
				int remoteId = new DataInputStream(newSocket.getInputStream()).readInt();

				// ******* EDUARDO BEGIN **************//
				if (!this.controller.isInCurrentView() && (this.controller.getStaticConf().getTTPId() != remoteId)) {
					waitViewLock.lock();
					pendingConn.add(new PendingConnection(newSocket, remoteId));
					waitViewLock.unlock();
				} else {
					LOGGER.info("I am {} establishConnection run!", this.controller.getStaticConf().getProcessId());
					establishConnection(newSocket, remoteId);
				}
				// ******* EDUARDO END **************//

			} catch (SocketTimeoutException ex) {
				// timeout on the accept... do nothing
			} catch (SocketException ex) {
				if (doWork) {
					LOGGER.error("Socket error occurred while accepting incoming connection! --[CurrentProcessId="
							+ this.controller.getStaticConf().getProcessId() + "]" + ex.getMessage(), ex);
					try {
						serverSocket.close();
					} catch (Exception e) {
					}
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e1) {
					}
					try {
						createServerSocket(controller);
					} catch (Exception e) {
						LOGGER.error("Retry to create server socket fail! --[CurrentProcessId="
								+ this.controller.getStaticConf().getProcessId() + "]" + ex.getMessage(), ex);
					}
				}
			} catch (Exception ex) {
				if (doWork) {
					LOGGER.error("Unexpected error occurred while accepting incoming connection! --[CurrentProcessId="
							+ this.controller.getStaticConf().getProcessId() + "]" + ex.getMessage(), ex);
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

		LOGGER.info("ServerCommunicationLayer stopped! --[" + this.controller.getStaticConf().getProcessId() + "]");
	}

	// ******* EDUARDO BEGIN **************//
	private void establishConnection(Socket newSocket, int remoteId) throws IOException {
		LOGGER.info("I am {}, remoteId = {} !", this.controller.getStaticConf().getProcessId(), remoteId);
		if ((this.controller.getStaticConf().getTTPId() == remoteId) || this.controller.isCurrentViewMember(remoteId)) {
			connectionsLock.lock();
			// System.out.println("Vai se conectar com: "+remoteId);
			if (this.connections.get(remoteId) == null) { // This must never happen!!!
				// first time that this connection is being established
				// System.out.println("THIS DOES NOT HAPPEN....."+remoteId);
				this.connections.put(remoteId,
						new ServerConnection(controller, newSocket, remoteId, messageInQueue, replica));
			} else {
				// reconnection
				this.connections.get(remoteId).reconnect(newSocket);
			}
			connectionsLock.unlock();

		} else {
			// System.out.println("Closing connection of: "+remoteId);
			newSocket.close();
		}
	}
	// ******* EDUARDO END **************//

	public static void setSocketOptions(Socket socket) throws SocketException {
		socket.setTcpNoDelay(true);
	}

	@Override
	public String toString() {
		String str = "inQueue=" + messageInQueue.toString();

		int[] activeServers = controller.getCurrentViewAcceptors();

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
