package bftsmart.tom.leaderchange;

import bftsmart.tom.core.TOMLayer;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TimestampTimer {

    private static final long SEND_PERIOD = 10000L;

    private volatile Set<Integer> remotes = new HashSet<>();

    private final ScheduledExecutorService sendThreadPool = Executors.newSingleThreadScheduledExecutor();

    private final Lock lock = new ReentrantLock();

    private int processId;

    private TOMLayer tomLayer;

    public TimestampTimer(TOMLayer tomLayer) {
        this.tomLayer = tomLayer;
        this.processId = tomLayer.controller.getStaticConf().getProcessId();
        int[] otherAcceptors = tomLayer.controller.getCurrentViewOtherAcceptors();
        for (int other : otherAcceptors) {
            this.remotes.add(other);
        }
    }

    public void start() {
        sendThreadPool.scheduleAtFixedRate(() -> {
            lock.lock();
            try {
                TimestampMessage timestampMessage = new TimestampMessage(processId, System.currentTimeMillis());
                int[] remoteIds = new int[remotes.size()];
                int index = 0;
                for (int id : remotes) {
                    remoteIds[index++] = id;
                }
                tomLayer.getCommunication().send(remoteIds, timestampMessage);
            } finally {
                lock.unlock();
            }
        }, SEND_PERIOD, SEND_PERIOD, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        sendThreadPool.shutdownNow();
    }

    public void remove(int remoteId) {
        lock.lock();
        try {
            if (!remotes.isEmpty()) {
                remotes.remove(remoteId);
                if (remotes.isEmpty()) {
                    shutdown();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean contain(int remoteId) {
        return remotes.contains(remoteId);
    }
}
