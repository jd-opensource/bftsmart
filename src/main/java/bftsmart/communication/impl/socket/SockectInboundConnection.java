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
package bftsmart.communication.impl.socket;

import java.io.IOException;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.communication.MessageQueue;
import bftsmart.communication.impl.IOChannel;
import bftsmart.reconfiguration.ViewTopology;

/**
 * 基于 Socket 实现的入站连接；
 * 
 * @author huanghaiquan
 *
 */
public class SockectInboundConnection extends AbstractStreamConnection {
	private static final Logger LOGGER = LoggerFactory.getLogger(SockectInboundConnection.class);

	private volatile SocketChannel socketChannel;

	public SockectInboundConnection(String realmName, ViewTopology viewTopology, int remoteId,
			MessageQueue messageInQueue) {
		super(realmName, viewTopology, remoteId, messageInQueue);
	}


	@Override
	protected IOChannel getIOChannel(long timeoutMillis) {
		SocketChannel chl = this.socketChannel;
		if (chl != null && !chl.isClosed()) {
			return socketChannel;
		}
		
		try {
			long startTs = System.currentTimeMillis();
			do {
				Thread.sleep(500);
				
				chl = this.socketChannel;
				if (chl != null && !chl.isClosed()) {
					return chl;
				}
			} while ((System.currentTimeMillis() - startTs) < timeoutMillis);
		} catch (InterruptedException e) {
		}
		return null;
	}

	@Override
	public boolean isAlived() {
		return socketChannel != null && !socketChannel.isClosed();
	}

	/**
	 * 接入新的 Socket 连接；
	 * 
	 * @param newSocket
	 * @throws IOException
	 */
	public synchronized void accept(Socket newSocket) throws IOException {
		// 先关闭旧的 Socket 对象；
		SocketChannel chl = this.socketChannel;
		if (chl != null) {
			this.socketChannel = null;
			try {
				chl.close();
			} catch (Exception e) {
				LOGGER.debug("Error occurred while closing the socket channel! --" + e.getMessage(), e);
			}
		}

		if (!isDoWork()) {
			newSocket.close();
			return;
		}
		// 更新 Socket；
		socketChannel = new SocketChannel(newSocket);

		LOGGER.debug("Accept new socket. --[me={}][remote={}]", ME, REMOTE_ID);
	}

	@Override
	public synchronized void close() {
		super.close();
		
		SocketChannel chl = socketChannel;
		if (chl != null) {
			socketChannel = null;
			try {
				chl.close();
			} catch (Exception e) {
			}
		}
	}
	
}
