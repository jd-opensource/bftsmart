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
	

	public IncomingSockectConnection(String realmName, ViewTopology viewTopology, int remoteId,
			MessageQueue messageInQueue) {
		super(realmName, viewTopology, remoteId, messageInQueue);
	}

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

	public void accept(Socket newSocket) {
		socket = newSocket;
		LOGGER.debug("Accept new socket. --[me={}][remote={}]", me, remoteId);
	}

}
