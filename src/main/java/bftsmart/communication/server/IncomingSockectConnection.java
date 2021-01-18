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
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.communication.queue.MessageQueue;
import bftsmart.reconfiguration.ViewTopology;
import utils.io.RuntimeIOException;

/**
 * This class represents a connection with other server.
 *
 * ServerConnections are created by ServerCommunicationLayer.
 *
 * @author alysson
 */
public class IncomingSockectConnection extends AbstractSockectConnection {

	private static final Logger LOGGER = LoggerFactory.getLogger(IncomingSockectConnection.class);

	private volatile Socket socket;
	private volatile DataOutputStream socketOutStream = null;
	private volatile DataInputStream socketInStream = null;
	

	/** Only used when there is no sender Thread */

	public IncomingSockectConnection(String realmName, ViewTopology viewTopology, int remoteId,
			MessageQueue messageInQueue) {
		super(realmName, viewTopology, remoteId, messageInQueue);

//
////		this.noMACs = new HashSet<Integer>();
//		// Connect to the remote process or just wait for the connection?
//		if (isToConnect()) {
//			// I have to connect to the remote server
//			try {
//				this.socket = new Socket(viewTopology.getStaticConf().getHost(remoteId),
//						viewTopology.getStaticConf().getServerToServerPort(remoteId));
//				SocketUtils.setSocketOptions(this.socket);
//				new DataOutputStream(this.socket.getOutputStream())
//						.writeInt(viewTopology.getStaticConf().getProcessId());
//
//			} catch (UnknownHostException ex) {
//				LOGGER.warn(
//						"Error occurred while creating connection to remote[" + remoteId + "]! --" + ex.getMessage(),
//						ex);
//			} catch (IOException ex) {
//				LOGGER.warn(
//						"Error occurred while creating connection to remote[" + remoteId + "]! --" + ex.getMessage(),
//						ex);
//			}
//		}
//		// else I have to wait a connection from the remote server

	}

//    public ServerConnection(ServerViewController controller, Socket socket, int remoteId,
//                            LinkedBlockingQueue<SystemMessage> inQueue, ServiceReplica replica) {
//
//        this.controller = controller;
//
//        this.socket = socket;
//
//        this.remoteId = remoteId;
//
//        this.inQueue = inQueue;
//
//        this.outQueue = new LinkedBlockingQueue<byte[]>(controller.getStaticConf().getOutQueueSize());
//
//        this.noMACs = new HashSet<Integer>();
//        // Connect to the remote process or just wait for the connection?
//        if (isToConnect()) {
//            //I have to connect to the remote server
//            try {
//                this.socket = new Socket(controller.getStaticConf().getHost(remoteId),
//                        controller.getStaticConf().getServerToServerPort(remoteId));
//                ServersCommunicationLayer.setSocketOptions(this.socket);
//                new DataOutputStream(this.socket.getOutputStream()).writeInt(controller.getStaticConf().getProcessId());
//
//            } catch (UnknownHostException ex) {
//                ex.printStackTrace();
//            } catch (IOException ex) {
//                ex.printStackTrace();
//            }
//        }
//        //else I have to wait a connection from the remote server
//
//        if (this.socket != null) {
//            try {
//                socketOutStream = new DataOutputStream(this.socket.getOutputStream());
//                socketInStream = new DataInputStream(this.socket.getInputStream());
//            } catch (IOException ex) {
//                LOGGER.debug("Error creating connection to "+remoteId);
//                ex.printStackTrace();
//            }
//        }
//
//        //******* EDUARDO BEGIN **************//
//        this.useSenderThread = controller.getStaticConf().isUseSenderThread();
//
//        if (useSenderThread && (controller.getStaticConf().getTTPId() != remoteId)) {
//            new SenderThread(latch).start();
//        } else {
//            sendLock = new ReentrantLock();
//        }
//        authenticateAndEstablishAuthKey();
//
//        if (!controller.getStaticConf().isTheTTP()) {
//            if (controller.getStaticConf().getTTPId() == remoteId) {
//                //Uma thread "diferente" para as msgs recebidas da TTP
//                new TTPReceiverThread(replica).start();
//            } else {
//                new ReceiverThread().start();
//            }
//        }
//        //******* EDUARDO END **************//
//    }

	// ******* EDUARDO BEGIN **************//
	// return true of a process shall connect to the remote process, false otherwise
	private boolean isToConnect() {
		boolean ret = false;
		if (this.viewTopology.isInCurrentView()) {

			// in this case, the node with higher ID starts the connection
			if (viewTopology.getStaticConf().getProcessId() > remoteId) {
				ret = true;
			}

		}
		return ret;
	}
	// ******* EDUARDO END **************//

	/**
	 * (Re-)establish connection between peers.
	 *
	 * @param newSocket socket created when this server accepted the connection
	 *                  (only used if processId is less than remoteId)
	 */
//	protected void reconnect(Socket newSocket) {
//		synchronized (connectLock) {
//			if (socket != null && socket.isConnected()) {
//				return;
//			}
//
//			try {
//				// ******* EDUARDO BEGIN **************//
//				if (isToConnect()) {
//					socket = new Socket(viewTopology.getStaticConf().getHost(remoteId),
//							viewTopology.getStaticConf().getServerToServerPort(remoteId));
//					SocketUtils.setSocketOptions(socket);
//					new DataOutputStream(socket.getOutputStream())
//							.writeInt(viewTopology.getStaticConf().getProcessId());
//
//					// ******* EDUARDO END **************//
//				} else {
//					socket = newSocket;
//				}
//			} catch (UnknownHostException ex) {
//				LOGGER.warn(
//						"Error occurred while reconnecting to remote replic[" + remoteId + "]! -- " + ex.getMessage(),
//						ex);
//			} catch (IOException ex) {
//				LOGGER.warn(
//						"Error occurred while reconnecting to remote replic[" + remoteId + "]! -- " + ex.getMessage(),
//						ex);
//			}
//
//			if (socket != null) {
//				try {
//					socketOutStream = new DataOutputStream(socket.getOutputStream());
//					socketInStream = new DataInputStream(socket.getInputStream());
//
//					authKey = null;
//					authenticateAndEstablishAuthKey();
//				} catch (IOException ex) {
//					LOGGER.error("Authentication fails while reconnect to remote replica[" + remoteId + "] ! --"
//							+ ex.getMessage(), ex);
//				}
//			}
//		}
//	}

	/**
	 * 关闭连接；此方法不抛出任何异常；
	 */
	@Override
	protected void closeSocket() {
		Socket sk = socket;
		DataOutputStream os = socketOutStream;
		DataInputStream is = socketInStream;

		socket = null;
		socketOutStream = null;
		socketInStream = null;

		if (os != null) {
			try {
				os.close();
			} catch (Exception e) {
			}
		}
		if (is != null) {
			try {
				is.close();
			} catch (Exception e) {
			}
		}
		if (sk != null) {
			try {
				sk.close();
			} catch (Exception e) {
			}
		}
	}

	@Override
	protected DataOutputStream getSocketOutputStream() {
		return socketOutStream;
	}

	@Override
	protected DataInputStream getSocketInputStream() {
		return socketInStream;
	}

	@Override
	protected void ensureConnection() {
		if (socket == null) {
			return;
		}
		if (socket.isClosed()) {
			socket = null;
			this.socketOutStream = null;
			this.socketInStream = null;
		}
		try {
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());
			this.socketOutStream = out;
			this.socketInStream = in;
		} catch (IOException e) {
			throw new RuntimeIOException(e.getMessage(), e);
		}
	}
	
	public boolean isAlived() {
		return socket != null && socket.isConnected();
	}

	public void acceptSocket(Socket newSocket) {
		socket = newSocket;
	}

}
