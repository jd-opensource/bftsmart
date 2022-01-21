package bftsmart.communication.impl.netty;

import bftsmart.communication.MacKey;
import bftsmart.communication.MacMessageCodec;
import bftsmart.communication.MessageQueue;
import bftsmart.communication.SystemMessage;
import bftsmart.communication.SystemMessageCodec;
import bftsmart.communication.impl.AsyncFuture;
import bftsmart.communication.impl.CompletedCallback;
import bftsmart.communication.impl.MessageConnection;
import bftsmart.communication.impl.MessageSendingTask;
import bftsmart.reconfiguration.ViewTopology;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;

public abstract class AbstractNettyConnection implements MessageConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractNettyConnection.class);
    // 每次连接的等待超时时长（毫秒）；
    private static final long CONNECTION_TIMEOUT = 20 * 1000;
    protected final String REALM_NAME;
    protected final int ME;
    protected final int REMOTE_ID;
    // 最大消息尺寸 100MB；
    protected final int MAX_MESSAGE_SIZE = 100 * 1024 * 1024;
    private final int MAX_RETRY_COUNT;
    protected ViewTopology viewTopology;
    protected ChannelHandlerContext context;

    private MessageQueue messageInQueue;
    private LinkedBlockingQueue<MessageSendingTask> outQueue;

    private SystemMessageCodec messageCodec;

    private volatile boolean doWork = false;
    private volatile Thread senderTread;

    public AbstractNettyConnection(String realmName, ViewTopology viewTopology, int remoteId, MessageQueue messageInQueue) {
        this.REALM_NAME = realmName;
        this.ME = viewTopology.getCurrentProcessId();
        this.REMOTE_ID = remoteId;

        this.messageCodec = new SystemMessageCodec();
        this.messageCodec.setUseMac(viewTopology.getStaticConf().isUseMACs());
        this.viewTopology = viewTopology;
        this.messageInQueue = messageInQueue;

        this.outQueue = new LinkedBlockingQueue<>(viewTopology.getStaticConf().getOutQueueSize());

        this.MAX_RETRY_COUNT = viewTopology.getStaticConf().getSendRetryCount();
        if (MAX_RETRY_COUNT < 1) {
            throw new IllegalArgumentException("Illegal SEND_RETRY_COUNT[" + MAX_RETRY_COUNT + "]!");
        }

        LOGGER.debug("Create netty connection from {} to {}!", ME, REMOTE_ID);
    }

    @Override
    public void start() {
        if (doWork) {
            return;
        }
        doWork = true;

        senderTread = new Thread(() -> scheduleSending(), "Sender-Thread-To-Remote[" + REMOTE_ID + "]");
        senderTread.setDaemon(true);

        senderTread.start();

        LOGGER.debug("Start connection! --[Me={}][Remote={}]", ME, REMOTE_ID);
    }

    @Override
    public AsyncFuture<SystemMessage, Void> send(SystemMessage message, boolean retrySending,
                                                 CompletedCallback<SystemMessage, Void> callback) {
        MessageSendingTask task = new MessageSendingTask(message, retrySending);
        task.setCallback(callback);

        if (!outQueue.offer(task)) {
            LOGGER.error("ServerConnection.send out queue for {} full message discarded.", REMOTE_ID);
            task.error(new IllegalStateException("ServerConnection.send out queue for {" + REMOTE_ID + "} full message discarded."));
        }

        return task;
    }

    @Override
    public int getRemoteId() {
        return REMOTE_ID;
    }

    @Override
    public MacMessageCodec<SystemMessage> getMessageCodec() {
        return messageCodec;
    }

    @Override
    public void clearSendingQueue() {
        outQueue.clear();
    }

    @Override
    public synchronized void close() {
        if (!doWork) {
            return;
        }

        doWork = false;
        try {
            senderTread.interrupt();
            try {
                senderTread.join();
            } catch (InterruptedException e) {
            }

            senderTread = null;
            ChannelHandlerContext chl = this.context;
            if (chl != null) {
                this.context = null;
                chl.close();
            }
        } catch (Exception e) {
            LOGGER.warn("Error occurred while closing connection! --[Me={}][Remote={}]", ME, REALM_NAME, e);
        }
        LOGGER.debug("Connection is closed! --[Me={}][Remote={}]", ME, REMOTE_ID);
    }

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
                LOGGER.error("Error occurred while sending message to remote[{}]!", REMOTE_ID, e);
            }
        }

        LOGGER.info("The sending task schedule of connection to remote[{}] stopped!", REMOTE_ID);
    }

    private final void processSendingTask(MessageSendingTask messageTask) {
        int retryCount = 0;
        do {
            // 检查连接；
            confirmConnection();

            // 当连接未建立时：
            // 对于无需重试发送的消息，则直接丢弃；
            // 对于需要重试发送的消息，则一直等待直到连接重新建立为止；
            if (null == context) {
                if (!messageTask.RETRY) {
                    // 抛弃连接；
                    messageTask.error(new IllegalStateException("Connection has not been established!"));
                    LOGGER.warn("Discard the message because connection has not been established and the task has no retry indication! --[Me={}][Remote={}]", ME, REMOTE_ID);
                    return;
                }

                if (retryCount >= MAX_RETRY_COUNT) {
                    // 抛弃连接；
                    messageTask.error(new IllegalStateException("Connection has not been established after retrying!"));
                    LOGGER.warn("Discard the message because connection has not been established after retrying! --[Me={}][Remote={}]", ME, REMOTE_ID);
                    return;
                }

                retryCount++;
                continue;
            }

            Exception error = null;
            try {
                // 将编码消息写入输出流；
                context.writeAndFlush(messageCodec.encode(messageTask.getSource()));

                // 发送任务成功；
                messageTask.complete(null);
                return;
            } catch (Exception ex) {
                error = ex;
            }

            // 如果不重试发送失败的消息，则立即报告错误；
            if (!messageTask.RETRY) {
                messageTask.error(error);
                LOGGER.error("Discard the message due to the io error and no retry indication! --" + error.getMessage(), error);
                return;
            }

            retryCount++;

            // 超过重试次数，报告错误；
            if (retryCount >= MAX_RETRY_COUNT) {
                LOGGER.error("Discard the message due to the io error after retrying! --[Me={}][Remote={}]", ME, REMOTE_ID, error);
                messageTask.error(error);
                return;
            }
        } while (doWork);

        messageTask.error(new IllegalStateException("Message has not sent because connection is shutdown!"));
    }

    private void confirmConnection() {
        if (isAlived()) {
            return;
        }
        try {
            long startTs = System.currentTimeMillis();
            do {
                Thread.sleep(500);
                if (isAlived()) {
                    return;
                }
            } while ((System.currentTimeMillis() - startTs) < CONNECTION_TIMEOUT);
        } catch (InterruptedException e) {
        }
    }

    @Override
    public boolean isAlived() {
        return null != context ? context.channel().isActive() : false;
    }

    /**
     * 接收消息
     *
     * @param bytes
     */
    protected void receiveMessage(byte[] bytes) {
        try {
            SystemMessage msg = messageCodec.decode(bytes);
            if (msg.getSender() == REMOTE_ID) {
                MessageQueue.SystemMessageType msgType = MessageQueue.SystemMessageType.typeOf(msg);
                if (!messageInQueue.offer(msgType, msg)) {
                    LOGGER.error("Discard message because the input queue is full! [Me={}][Remote={}]", ME, REMOTE_ID);
                }
            } else {
                LOGGER.error("Discard the received message from wrong sender!  --[Sender={}][ExpectedSender={}][Me={}]", msg.getSender(), REMOTE_ID, ME);
            }
        } catch (Exception e) {
            LOGGER.error("Message handler fail! --[Me={}][Remote={}]", ME, REMOTE_ID, e);
            return;
        }
    }

    /**
     * 绑定连接通道
     *
     * @param ctx
     * @param macKey
     */
    protected synchronized void attachChannelHandlerContext(ChannelHandlerContext ctx, MacKey macKey) {
        if (null != context) {
            context.close();
        }
        this.context = ctx;
        messageCodec.setMacKey(macKey);
    }
}
