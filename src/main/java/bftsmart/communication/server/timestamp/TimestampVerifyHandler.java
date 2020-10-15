package bftsmart.communication.server.timestamp;

import bftsmart.communication.MessageHandler;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.tom.ServiceReplica;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class TimestampVerifyHandler implements TimestampVerifyService {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(MessageHandler.class);

    /**
     * 最长等待时间
     *         默认3分钟
     *
     */
    private static final long MAX_WAIT_SECONDS = 3 * 60L;

    /**
     * 关闭整个共识网络前等待的时间
     *         默认3秒
     *
     */
    private static final long KILL_WAIT_SECONDS = 3;

    private final CompletableFuture<Long> timestampFuture = new CompletableFuture<>();

    private final ServiceReplica replica;

    private final int processId;

    private final long timeTolerance;

    private final CountDownLatch timestampWaitLatch;

    private final CountDownLatch timestampSuccessLatch;

    private final AtomicBoolean timestampCompleted = new AtomicBoolean(false);

    private final AtomicBoolean killAtomic = new AtomicBoolean(false);

    public TimestampVerifyHandler(ServiceReplica replica, ServerViewController controller) {
        this.replica = replica;
        int remoteSize = controller.getCurrentViewOtherAcceptors().length;
        this.processId = controller.getStaticConf().getProcessId();
        this.timeTolerance = controller.getStaticConf().getTimeTolerance();
        this.timestampWaitLatch = new CountDownLatch(remoteSize);
        this.timestampSuccessLatch = new CountDownLatch(remoteSize);
        Executors.newSingleThreadExecutor().execute(() -> {
            try {
                // 等待网络处理完成
                timestampWaitLatch.await();
                LOGGER.info("I am {}, I will set timestamp", processId);
                // 设置当前时间
                long currentTimeMillis = System.currentTimeMillis();
                LOGGER.info("I am {}, set currentTime = {} !", processId, currentTimeMillis);
                timestampFuture.complete(currentTimeMillis);
                // 在指定时间内等待处理完成时间同步
                boolean result = timestampSuccessLatch.await(MAX_WAIT_SECONDS, TimeUnit.SECONDS);
                if (!result) {
                    kill();
                } else {
                    LOGGER.info("I am {}, handle timestamp verify success !", processId);
                    // 设置为完成
                    timestampCompleted.set(true);
                }
            } catch (Exception e) {
                LOGGER.error("Handle timestamp verify error !!!", e);
            }
        });
    }

    @Override
    public boolean timeVerifyCompleted() {
        return timestampCompleted.get();
    }

    @Override
    public void waitComplete(int remoteId) {
        LOGGER.info("I am {}, complete socket with {} !", processId, remoteId);
        timestampWaitLatch.countDown();
    }

    @Override
    public void verifySuccess(int remoteId) {
        LOGGER.info("I am {}, verify timestamp success with {} !", processId, remoteId);
        timestampSuccessLatch.countDown();
        if (timestampSuccessLatch.getCount() <= 0) {
            timestampCompleted.set(true);
        }
    }

    @Override
    public void verifyFail(int remoteId) {
        LOGGER.error("I am {}, verify timestamp fail with {} !", processId, remoteId);
        try {
            kill();
        } catch (Exception e) {
            LOGGER.error("Kill Service Replica Error !!!", e);
        }
    }

    @Override
    public long verifyTimestamp() {
        try {
            return timestampFuture.get();
        } catch (Exception e) {
            LOGGER.error("Load timestamp Error !!!", e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean verifyTime(long localTime, int remoteId, long remoteTime) {
        if (Math.abs(localTime - remoteTime) < timeTolerance) {
            // 时间条件满足
            return true;
        }
        LOGGER.info("I am {}, localTime = {}, remoteTime = {}, remoteId = {} !", processId, localTime, remoteTime, remoteId);
        return false;
    }

    private void kill() throws Exception {
        boolean compareAndSet = killAtomic.compareAndSet(false, true);
        if (compareAndSet) {
            LOGGER.error("\r\n=================== TIMESTAMP VERIFY FAIL ===================\r\n" +
                         "I am {}, handle timestamp verify fail, I will kill myself !!!\r\n" +
                         "=============================================================", processId);
            // 等待3秒，然后关闭
            TimeUnit.SECONDS.sleep(KILL_WAIT_SECONDS);
            // 关闭
            replica.kill();
        }
    }
}
