package bftsmart.communication.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.communication.IllegalMessageException;
import bftsmart.communication.MacAuthenticationException;
import bftsmart.communication.MacAuthenticator;
import bftsmart.communication.MacKey;
import bftsmart.communication.MacKeyGenerator;
import bftsmart.communication.MacMessageCodec;
import bftsmart.communication.MessageAuthenticationException;
import bftsmart.communication.SystemMessage;
import bftsmart.communication.SystemMessageCodec;
import bftsmart.communication.queue.MessageQueue;
import bftsmart.reconfiguration.ViewTopology;
import utils.io.BytesUtils;

/**
 * AbstractStreamConnection 实现了基于流的消息连接对象；
 * 
 * @author huanghaiquan
 *
 */
public abstract class AbstractStreamConnection implements MessageConnection {

	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStreamConnection.class);

	// 每次连接的等待超时时长（毫秒）；
	private static final long CONNECTION_TIMEOUT = 20 * 1000;

	/**
	 * 最大消息尺寸 100MB；
	 */
	private final int MAX_MESSAGE_SIZE = 100 * 1024 * 1024;

	private final int MAX_RETRY_COUNT;

	protected final String REALM_NAME;
	protected final int ME;
	protected final int REMOTE_ID;

	protected ViewTopology viewTopology;

	private MacAuthenticator macAuthenticator;

	private MessageQueue messageInQueue;
	private LinkedBlockingQueue<MessageSendingTask> outQueue;

	private SystemMessageCodec messageCodec;

	private final Object channelMutex = new Object();
	private volatile IOChannel ioChannel;

	private volatile boolean doWork = false;

	private volatile Thread senderTread;

	private volatile Thread receiverThread;

	public AbstractStreamConnection(String realmName, ViewTopology viewTopology, int remoteId,
			MessageQueue messageInQueue) {
		this.REALM_NAME = realmName;
		this.ME = viewTopology.getCurrentProcessId();
		this.REMOTE_ID = remoteId;

		this.messageCodec = new SystemMessageCodec();
		this.messageCodec.setUseMac(viewTopology.getStaticConf().isUseMACs());
		this.viewTopology = viewTopology;
		this.messageInQueue = messageInQueue;

		MacKeyGenerator macKeyGen = new MacKeyGenerator(viewTopology.getStaticConf().getRSAPublicKey(),
				viewTopology.getStaticConf().getRSAPrivateKey(), viewTopology.getStaticConf().getDHG(),
				viewTopology.getStaticConf().getDHP());
		this.macAuthenticator = new MacAuthenticator(remoteId, viewTopology.getStaticConf().getRSAPublicKey(remoteId),
				macKeyGen);

		this.outQueue = new LinkedBlockingQueue<MessageSendingTask>(viewTopology.getStaticConf().getOutQueueSize());

		this.MAX_RETRY_COUNT = viewTopology.getStaticConf().getSendRetryCount();
		if (MAX_RETRY_COUNT < 1) {
			throw new IllegalArgumentException("Illegal SEND_RETRY_COUNT[" + MAX_RETRY_COUNT + "]!");
		}

		LOGGER.debug("Create stream connection from {} to {}!", ME, REMOTE_ID);
	}

	protected boolean isDoWork() {
		return doWork;
	}

	@Override
	public synchronized void start() {
		if (doWork) {
			return;
		}
		doWork = true;

		senderTread = new Thread(new Runnable() {
			public void run() {
				scheduleSending();
			}
		}, "Sender-Thread-To-Remote[" + REMOTE_ID + "]");
		senderTread.setDaemon(true);

		receiverThread = new Thread(new Runnable() {
			@Override
			public void run() {
				scheduleReceiving();
			}
		}, "Receiver-Thread-From-Remote[" + REMOTE_ID + "]");
		receiverThread.setDaemon(true);

		senderTread.start();
		receiverThread.start();

		LOGGER.debug("Start connection! --[Me={}][Remote={}]", ME, REMOTE_ID);
	}

	@Override
	public int getRemoteId() {
		return REMOTE_ID;
	}

	@Override
	public MacMessageCodec<SystemMessage> getMessageCodec() {
		return messageCodec;
	}

	/**
	 * Stop message sending and reception.
	 */
	@Override
	public synchronized void close() {
		if (!doWork) {
			return;
		}

		doWork = false;

		try {
			senderTread.interrupt();
			receiverThread.interrupt();
			try {
				senderTread.join();
			} catch (InterruptedException e) {
			}
			try {
				receiverThread.join();
			} catch (InterruptedException e) {
			}

			senderTread = null;
			receiverThread = null;

			IOChannel chl = this.ioChannel;
			if (chl != null) {
				this.ioChannel = null;
				chl.close();
			}
		} catch (Exception e) {
			LOGGER.warn(String.format("Error occurred while closing connection! --[Me=%s][Remote=%s] %s", ME,
					REALM_NAME, e.getMessage()), e);
		}

		LOGGER.debug("Connection is closed! --[Me={}][Remote={}]", ME, REMOTE_ID);
	}

	@Override
	public void clearSendingQueue() {
		outQueue.clear();
	}

	/**
	 * Used to send packets to the remote server.
	 */
	/**
	 * @param data         要发送的数据；
	 * @param useMAC       是否使用 MAC；
	 * @param retrySending 当发送失败时，是否要重试；
	 * @param callback     发送完成回调；
	 * @return
	 * @throws InterruptedException
	 */
	@Override
	public AsyncFuture<SystemMessage, Void> send(SystemMessage message, boolean retrySending,
			CompletedCallback<SystemMessage, Void> callback) {
		MessageSendingTask task = new MessageSendingTask(message, retrySending);
		task.setCallback(callback);

		if (!outQueue.offer(task)) {
			LOGGER.error("(ServerConnection.send) out queue for {} full (message discarded).", REMOTE_ID);

			task.error(new IllegalStateException(
					"(ServerConnection.send) out queue for {" + REMOTE_ID + "} full (message discarded)."));
		}

		return task;
	}

	/**
	 * 驻留后台线程，执行消息发送；
	 */
	private final void scheduleSending() {
		MessageSendingTask task;
		while (doWork) {
			try {
				// 检查发送队列；
				task = null;
				try {
					task = outQueue.take();
				} catch (InterruptedException ex) {
				}

				if (task != null) {
					// 处理发送任务；
					processSendingTask(task);
				}
			} catch (Exception e) {
				LOGGER.error("Error occurred while sending message to remote[" + REMOTE_ID + "]! --" + e.getMessage(),
						e);
			}
		}

		LOGGER.info("The sending task schedule of connection to remote[{}] stopped!", REMOTE_ID);
	}

	/**
	 * try to send a message through the socket if some problem is detected, a
	 * reconnection is done
	 */
	private final void processSendingTask(MessageSendingTask messageTask) {
		// 生成消息的输出编码；
		byte[] outputBytes = null;

		int retryCount = 0;
		OutputStream out = null;
		do {
			try {
				// 检查连接；
				try {
					if (out == null) {
						out = getOutputStream(CONNECTION_TIMEOUT);
					}
				} catch (Exception e) {
					// 建立连接时发生网络IO错误；
					LOGGER.error("Error occurred while connecting to remote! --[Me=" + ME + "][Remote=" + REMOTE_ID
							+ "] " + e.getMessage(), e);
					out = null;
				}

				// 当连接未建立时：
				// 对于无需重试发送的消息，则直接丢弃；
				// 对于需要重试发送的消息，则一直等待直到连接重新建立为止；
				if (out == null) {
					if (!messageTask.RETRY) {
						// 抛弃连接；
						messageTask.error(new IllegalStateException("Connection has not been established!"));
						LOGGER.warn(
								"Discard the message because connection has not been established and the task has no retry indication! --[Me={}][Remote={}]",
								ME, REMOTE_ID);
						return;
					}

					if (retryCount >= MAX_RETRY_COUNT) {
						// 抛弃连接；
						messageTask.error(
								new IllegalStateException("Connection has not been established after retrying!"));
						LOGGER.warn(
								"Discard the message because connection has not been established after retrying! --[Me={}][Remote={}]",
								ME, REMOTE_ID);
						return;
					}

					retryCount++;
					continue;
				}

				IOException error = null;
				// if there is a need to reconnect, abort this method
				try {
					// 连接已准备就绪，并通过了 MAC 认证；
					// 基于连接认证生成的 MAC 共享密钥对消息进行编码输出；
					outputBytes = messageCodec.encode(messageTask.getSource());

					// 将编码消息写入输出流；
					BytesUtils.writeInt(outputBytes.length, out);
					out.write(outputBytes);
					out.flush();

					// 发送任务成果；
					messageTask.complete(null);
					return;
				} catch (IOException ex) {
					try {
						out.close();
					} catch (Exception e) {
					}
					out = null;
					error = ex;
				}

				// 写数据时发生网络IO错误；
				// 如果不重试发送失败的消息，则立即报告错误；
				if (!messageTask.RETRY) {
					messageTask.error(error);
					LOGGER.error(
							"Discard the message due to the io error and no retry indication! --" + error.getMessage(),
							error);
					return;
				}

				// 如果不重试发送失败的消息，则立即报告错误；
				if (retryCount++ >= MAX_RETRY_COUNT) {
					LOGGER.error("Discard the message due to the io error after retrying! --[Me=" + ME + "][Remote="
							+ REMOTE_ID + "]" + error.getMessage(), error);
					messageTask.error(error);
					return;
				}

				// 重试；
				out = null;
				retryCount++;

			} catch (Exception e) {
				// 发生了未知的错误，不必重试，直接丢弃消息；
				LOGGER.error("Discard the message due to the unknown error! --[Me=" + ME + "][Remote=" + REMOTE_ID + "]"
						+ e.getMessage(), e);
				messageTask.error(e);
				return;
			}
		} while (doWork && retryCount < MAX_RETRY_COUNT);

		messageTask.error(new IllegalStateException("Completed in unexpected state!"));
	}

	/**
	 * 驻留后台线程，执行消息接收；
	 */
	private void scheduleReceiving() {
		InputStream in = null;
		while (doWork) {
			// 检查连接；
			try {
				if (in == null) {
					in = getInputStream(CONNECTION_TIMEOUT);
				}
			} catch (Exception e) {
				// 建立连接时发生网络IO错误；重试建立连接；
				LOGGER.error("Error occurred while connecting to remote! --[Me=" + ME + "][Remote=" + REMOTE_ID + "] "
						+ e.getMessage(), e);
				continue;
			}
			if (in == null) {
				// 重试，直到建立连接；
				continue;
			}

			try {
				// read message;
				SystemMessage sm = null;
				try {
					sm = readMessage(in);
				} catch (Exception e) {
					// 接收消息时发生网络错误；需要重新建立连接；
					LOGGER.error("Error occurred while reading the input message! --[Me=" + ME + "][Remote=" + REMOTE_ID
							+ "] " + e.getMessage(), e);
					try {
						in.close();
					} catch (Exception e1) {
					}
					in = null;
					continue;
				}

				if (sm == null) {
					continue;
				}
				if (sm.getSender() == REMOTE_ID) {
					MessageQueue.SystemMessageType msgType = MessageQueue.SystemMessageType.typeOf(sm);
					if (!messageInQueue.offer(msgType, sm)) {
						LOGGER.error("Discard message because the input queue is full! [Me={}][Remote={}]", ME,
								REMOTE_ID);
					}
				} else {
					LOGGER.error(
							"Discard the received message from wrong sender!  --[Sender={}][ExpectedSender={}][Me={}]",
							sm.getSender(), REMOTE_ID, ME);
				}
			} catch (Exception e) {
				LOGGER.error("Unknown error occurred! --[Me=" + ME + "][Remote=" + REMOTE_ID + "] " + e.getMessage(),
						e);
			}
		} // End of: while (doWork);
	}// End of : private void scheduleReceivingTask()

	/**
	 * 从输入流读消息；
	 * <p>
	 * 
	 * 如果输入流发生错误，则抛出 {@link IOException}；
	 * 
	 * @param in
	 * @return
	 * @throws IOException
	 */
	private SystemMessage readMessage(InputStream in) throws IOException {
		// 读消息字节；
//		int length = in.readInt();
		int length = BytesUtils.readInt(in);
		if (length > MAX_MESSAGE_SIZE) {
			LOGGER.error("Illegal message size! --[MessageSize={}][Me={}]", length, ME);
			throw new IOException("Illegal message size[" + length + "]!");
		}
		byte[] encodedMessageBytes = new byte[length];
		int read = 0;
		do {
			read += in.read(encodedMessageBytes, read, length - read);
		} while (read < length);

		try {
			return messageCodec.decode(encodedMessageBytes);
		} catch (MessageAuthenticationException | IllegalMessageException e) {
			String errMsg = String.format("The MAC Validation of the received message fail! --[Me=%s][Remote=%s] %s",
					ME, REMOTE_ID, e.getMessage());
			LOGGER.error(errMsg, e);
			return null;
		}

	}

	/**
	 * 获取经过认证的通过；
	 * 
	 * @param timeoutMillis
	 * @return
	 * @throws IOException
	 * @throws MacAuthenticationException
	 */
	private IOChannel getAuthenticatedChannel(long timeoutMillis) throws MacAuthenticationException, IOException {
		if (ioChannel == null || ioChannel.isClosed()) {
			synchronized (channelMutex) {
				if (ioChannel == null || ioChannel.isClosed()) {
					IOChannel chl = getIOChannel(timeoutMillis);
					if (chl == null) {
						ioChannel = null;
						messageCodec.setMacKey(null);
						return null;
					}
					try {
						MacKey macKey = macAuthenticator.authenticate(chl);
						ioChannel = chl;
						messageCodec.setMacKey(macKey);
					} catch (MacAuthenticationException | IOException e) {
						// 认证失败，直接关闭通道，并报告异常；
						ioChannel = null;
						messageCodec.setMacKey(null);

						try {
							chl.close();
						} catch (Exception e1) {
						}
						throw e;
					}
				}
			}
		}
		return ioChannel;
	}

	/**
	 * 返回用于发送数据的输出流；
	 * <p>
	 * 
	 * 如果连接未就绪，将堵塞当前线程进行等待，直到超时或者连接已就绪；
	 * <p>
	 * 
	 * 如果连接未就绪，或者等待超时，则返回 null；
	 * <p>
	 * 
	 * @return
	 * @throws IOException
	 * @throws MacAuthenticationException
	 */
	private OutputStream getOutputStream(long timeoutMillis) throws MacAuthenticationException, IOException {
		IOChannel channel = getAuthenticatedChannel(timeoutMillis);
		if (channel != null) {
			return channel.getOutputStream();
		}
		return null;
	}

	/**
	 * 获取用于接收数据的输入流；
	 * <p>
	 * 
	 * 如果连接未就绪，将堵塞当前线程进行等待，直到超时或者连接已就绪；
	 * <p>
	 * 
	 * 如果连接未就绪，或者等待超时，则返回 null；
	 * <p>
	 * 
	 * @param timeoutMillis
	 * @return
	 * @throws IOException
	 * @throws MacAuthenticationException
	 */
	private InputStream getInputStream(long timeoutMillis) throws MacAuthenticationException, IOException {
		IOChannel channel = getAuthenticatedChannel(timeoutMillis);
		if (channel != null) {
			return channel.getInputStream();
		}
		return null;
	}

	/**
	 * 返回用于消息通讯的输入输出通道；
	 * <p>
	 * 如果连接未就绪，将堵塞当前线程进行等待，直到超时或者连接已就绪；
	 * <p>
	 * 
	 * 如果连接未就绪，或者等待超时，则返回 null；
	 * <p>
	 * 
	 * 当前类型（{@link AbstractStreamConnection}）的实现中，当需要执行重置输入输出的操作时，会遵循以下步骤：<br>
	 * 1. 关闭输入输出流 ({@link OutputStream#close()} 或 {@link InputStream#close()} 或
	 * {@link IOChannel#close()})；<br>
	 * 2. 重新取输入输出流 ({@link #getOutputStream(long)} 或 {@link #getInputStream(long)} 或
	 * {@link #getIOChannel(long)})；<br>
	 * <p>
	 * 实现者需遵循这一约定提供相应实现，确保关闭操作能够正确地释放资源；
	 * 
	 * @param timeoutMillis
	 * @return
	 */
	protected abstract IOChannel getIOChannel(long timeoutMillis);

	@Override
	public String toString() {
		return this.getClass().getName() + " To [" + REMOTE_ID + "]";
	}

	// ---------------------

	/**
	 * 消息发送任务；
	 * 
	 * @author huanghaiquan
	 *
	 */
	private static class MessageSendingTask extends AsyncFutureTask<SystemMessage, Void> {

		public final boolean RETRY;

		public MessageSendingTask(SystemMessage message, boolean retry) {
			super(message);
			this.RETRY = retry;
		}

	}

}
