package bftsmart.communication.server.socket;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.communication.queue.MessageQueue;
import bftsmart.communication.server.SocketUtils;
import bftsmart.reconfiguration.ViewTopology;

/**
 * 基于 Socket 实现的出站连接；
 * 
 * @author huanghaiquan
 *
 */
public class SockectOutboundConnection extends AbstractStreamConnection {

	private static final Logger LOGGER = LoggerFactory.getLogger(SockectOutboundConnection.class);

	private volatile Socket socket;
	private volatile DataOutputStream socketOutStream = null;
	private volatile DataInputStream socketInStream = null;

	public SockectOutboundConnection(String realmName, ViewTopology viewTopology, int remoteId,
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
	protected synchronized void rebuildConnection(long timeoutMillis) throws IOException {
		closeConnection();

		long startTs = System.currentTimeMillis();

		IOException error = null;
		do {
			try {
				Socket sc = connectToRemote();
				DataOutputStream out = new DataOutputStream(sc.getOutputStream());
				DataInputStream in = new DataInputStream(sc.getInputStream());

				this.socketOutStream = out;
				this.socketInStream = in;
				this.socket = sc;

				return;
			} catch (IOException e) {
				LOGGER.debug("Error occurred while connecting to remote! --[Me=" + ME + "][Remote=" + REMOTE_ID + "] "
						+ e.getMessage(), e);
				error = e;
			}
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				return;
			}
		} while ((System.currentTimeMillis() - startTs) < timeoutMillis);

		// 连接出错，并且重试超时；
		throw error;
	}

	private Socket connectToRemote() throws UnknownHostException, IOException {
		Socket sc = new Socket(viewTopology.getStaticConf().getHost(REMOTE_ID),
				viewTopology.getStaticConf().getServerToServerPort(REMOTE_ID));
		SocketUtils.setSocketOptions(sc);

		return sc;
	}

	/**
	 * 连接是否有效；
	 */
	@Override
	public boolean isAlived() {
		return socket != null && socket.isConnected();
	}

}
