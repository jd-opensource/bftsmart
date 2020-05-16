package bftsmart.communication.queue;

import bftsmart.communication.SystemMessage;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MessageInQueue implements MessageQueue {

    private final LinkedBlockingQueue<SystemMessage> consMsgQueue;

    private final LinkedBlockingQueue<SystemMessage> lcMsgQueue;

    private final LinkedBlockingQueue<SystemMessage> heartMsgQueue;

    public MessageInQueue(int capacity){
        consMsgQueue = new LinkedBlockingQueue<>(capacity);
        lcMsgQueue = new LinkedBlockingQueue<>(capacity);
        heartMsgQueue = new LinkedBlockingQueue<>(capacity);
    }

    @Override
    public boolean offer(MSG_TYPE type, SystemMessage sm) {
        if (type == MSG_TYPE.CONSENSUS) {
            return consMsgQueue.offer(sm);
        } else if (type == MSG_TYPE.HEART) {
            return heartMsgQueue.offer(sm);
        } else if (type == MSG_TYPE.LC) {
            return lcMsgQueue.offer(sm);
        }
        return false;
    }

    @Override
    public void put(MSG_TYPE type, SystemMessage sm) throws InterruptedException {
        if (type == MSG_TYPE.CONSENSUS) {
            consMsgQueue.put(sm);
        } else if (type == MSG_TYPE.HEART) {
            heartMsgQueue.put(sm);
        } else if (type == MSG_TYPE.LC) {
            lcMsgQueue.put(sm);
        }
    }

    @Override
    public SystemMessage poll(MSG_TYPE type, long timeout, TimeUnit unit) throws InterruptedException {
        if (type == MSG_TYPE.CONSENSUS) {
            return consMsgQueue.poll(timeout, unit);
        } else if (type == MSG_TYPE.HEART) {
            return heartMsgQueue.poll(timeout, unit);
        } else if (type == MSG_TYPE.LC) {
            return lcMsgQueue.poll(timeout, unit);
        }
        return null;
    }
}
