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
 * 
 * @author huanghaiquan
 *
 */
public class OutgoingSockectConnection extends AbstractSockectConnection {

	private static final Logger LOGGER = LoggerFactory.getLogger(OutgoingSockectConnection.class);

	private volatile Socket socket;
	private volatile DataOutputStream socketOutStream = null;
	private volatile DataInputStream socketInStream = null;

	/** Only used when there is no sender Thread */

	public OutgoingSockectConnection(String realmName, ViewTopology viewTopology, int remoteId,
			MessageQueue messageInQueue) {
		super(realmName, viewTopology, remoteId, messageInQueue);
	}

	/**
	 * 关闭连接；此方法不抛出任何异常；
	 */
	@Override
	protected void closeSocket() {
		Socket sc = socket;
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
	protected DataOutputStream getSocketOutputStream() {
		return socketOutStream;
	}

	@Override
	protected DataInputStream getSocketInputStream() {
		return socketInStream;
	}

	@Override
	protected void ensureConnection() {
		if (isAlived()) {
			return;
		}
		if (socket != null) {
			closeSocket();
		}
		
		Socket sc = connect();
		if (sc == null) {
			return;
		}
		try {
			DataOutputStream out = new DataOutputStream(sc.getOutputStream());
			DataInputStream in = new DataInputStream(sc.getInputStream());
			
			this.socketOutStream = out;
			this.socketInStream = in;
			this.socket = sc;
		} catch (IOException e) {
			throw new RuntimeIOException(e.getMessage(), e);
		}
	}

	private Socket connect() {
		// I have to connect to the remote server
		try {
			Socket sc = new Socket(viewTopology.getStaticConf().getHost(remoteId),
					viewTopology.getStaticConf().getServerToServerPort(remoteId));
			SocketUtils.setSocketOptions(sc);

			return sc;
		} catch (IOException e) {
			LOGGER.error("Error occurred while connect to remote! --{} --[CurrentId={}][RemoteId={}]",
					viewTopology.getCurrentProcessId(), remoteId);
			return null;
		}
	}

	public boolean isAlived() {
		return socket != null && socket.isConnected();
	}

}
