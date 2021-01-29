package bftsmart.communication.server.socket;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.communication.queue.MessageQueue;
import bftsmart.communication.server.AbstractStreamConnection;
import bftsmart.communication.server.IOChannel;
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

	private volatile SocketChannel socketChannel;

	public SockectOutboundConnection(String realmName, ViewTopology viewTopology, int remoteId,
			MessageQueue messageInQueue) {
		super(realmName, viewTopology, remoteId, messageInQueue);
	}

	@Override
	protected IOChannel getIOChannel(long timeoutMillis) {
		if (socketChannel != null && !socketChannel.isClosed()) {
			return socketChannel;
		}
		try {
			socketChannel.close();
		} catch (Exception e) {
		}
		socketChannel = null;
		
		socketChannel = reconnect(timeoutMillis);
		return socketChannel;
	}

	/**
	 * 重连；
	 * <p>
	 * 
	 * 如果超时尚未连接成功，则返回 null;
	 * 
	 * @param timeoutMillis
	 * @return
	 */
	private SocketChannel reconnect(long timeoutMillis) {
		long startTs = System.currentTimeMillis();

		do {
			try {
				Socket sc = connectToRemote();
				
				// 发送当前节点的 Id 到服务端，进行识别；
				sendCurrentId(sc);
				
				return new SocketChannel(sc);
			} catch (Exception e) {
				LOGGER.warn("Error occurred while connecting to remote! --[Me=" + ME + "][Remote=" + REMOTE_ID + "] "
						+ e.getMessage(), e);
			}

			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				return null;
			}
		} while ((System.currentTimeMillis() - startTs) < timeoutMillis);

		return null;
	}

	private void sendCurrentId(Socket sc) throws IOException {
		DataOutputStream out = new DataOutputStream(sc.getOutputStream());
		out.writeInt(ME);
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
		return socketChannel != null && !socketChannel.isClosed();
	}

}
