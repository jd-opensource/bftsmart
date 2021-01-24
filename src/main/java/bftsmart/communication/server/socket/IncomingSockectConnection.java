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
package bftsmart.communication.server.socket;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.communication.queue.MessageQueue;
import bftsmart.reconfiguration.ViewTopology;

/**
 * This class represents a connection with other server.
 *
 * ServerConnections are created by ServerCommunicationLayer.
 *
 * @author alysson
 */
public class IncomingSockectConnection extends AbstractStreamConnection {
	private static final Logger LOGGER = LoggerFactory.getLogger(IncomingSockectConnection.class);

	private volatile Socket socket;
	private volatile DataOutputStream socketOutStream = null;
	private volatile DataInputStream socketInStream = null;

	private Semaphore acceptor = new Semaphore(0);

	public IncomingSockectConnection(String realmName, ViewTopology viewTopology, int remoteId,
			MessageQueue messageInQueue) {
		super(realmName, viewTopology, remoteId, messageInQueue);
	}

	/**
	 * 关闭连接；此方法不抛出任何异常；
	 */
	@Override
	protected synchronized void closeConnection() {
		Socket sc = socket;
		if (sc == null) {
			return;
		}
		
		DataOutputStream out = socketOutStream;
		DataInputStream in = socketInStream;
		
		socket = null;
		socketOutStream = null;
		socketInStream = null;

		if (out != null) {
			try {
				out.close();
			} catch (Exception e) {
			}
		}
		if (in != null) {
			try {
				in.close();
			} catch (Exception e) {
			}
		}
		if (sc != null) {
			try {
				sc.close();
			} catch (Exception e) {
			}
		}
	}

	@Override
	protected DataOutputStream getOutputStream() {
		return socketOutStream;
	}

	@Override
	protected DataInputStream getInputStream() {
		return socketInStream;
	}

	@Override
	protected void rebuildConnection(long timeout) {
		int v = acceptor.drainPermits();
		if (v > 0) {
			return;
		}
		try {
			acceptor.tryAcquire(timeout, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
		}
	}

	public boolean isAlived() {
		return socket != null && socket.isConnected();
	}

	/**
	 * 接入新的 Socket 连接；
	 * 
	 * @param newSocket
	 * @throws IOException 
	 */
	public synchronized void accept(Socket newSocket) throws IOException {
		// 先关闭旧的 Socket 对象；
		closeConnection();
		
		if (!isDoWork()) {
			newSocket.close();
			return;
		}
		// 更新 Socket；
		DataOutputStream out = new DataOutputStream(newSocket.getOutputStream());
		DataInputStream in = new DataInputStream(newSocket.getInputStream());
		
		this.socketOutStream = out;
		this.socketInStream = in;
		this.socket = newSocket;
		
		acceptor.release();
		
		LOGGER.debug("Accept new socket. --[me={}][remote={}]", ME, REMOTE_ID);
	}

}
