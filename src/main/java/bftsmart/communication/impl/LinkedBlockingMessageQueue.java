package bftsmart.communication.impl;

import bftsmart.communication.MessageQueue;
import bftsmart.communication.SystemMessage;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class LinkedBlockingMessageQueue implements MessageQueue {

    private final LinkedBlockingQueue<SystemMessage> consMsgQueue;

    private final LinkedBlockingQueue<SystemMessage> lcMsgQueue;

    private final LinkedBlockingQueue<SystemMessage> heartMsgQueue;

    public LinkedBlockingMessageQueue(int capacity){
        consMsgQueue = new LinkedBlockingQueue<>(capacity);
        lcMsgQueue = new LinkedBlockingQueue<>(capacity);
        heartMsgQueue = new LinkedBlockingQueue<>(capacity);
    }

    @Override
    public boolean offer(SystemMessageType type, SystemMessage sm) {
        if (type == SystemMessageType.CONSENSUS) {
            return consMsgQueue.offer(sm);
        } else if (type == SystemMessageType.HEART) {
            return heartMsgQueue.offer(sm);
        } else if (type == SystemMessageType.LC) {
            return lcMsgQueue.offer(sm);
        }
        return false;
    }

    @Override
    public void put(SystemMessageType type, SystemMessage sm) throws InterruptedException {
        if (type == SystemMessageType.CONSENSUS) {
            consMsgQueue.put(sm);
        } else if (type == SystemMessageType.HEART) {
            heartMsgQueue.put(sm);
        } else if (type == SystemMessageType.LC) {
            lcMsgQueue.put(sm);
        }
    }

    @Override
    public SystemMessage poll(SystemMessageType type, long timeout, TimeUnit unit) throws InterruptedException {
        if (type == SystemMessageType.CONSENSUS) {
            return consMsgQueue.poll(timeout, unit);
        } else if (type == SystemMessageType.HEART) {
            return heartMsgQueue.poll(timeout, unit);
        } else if (type == SystemMessageType.LC) {
            return lcMsgQueue.poll(timeout, unit);
        }
        return null;
    }
    
    @Override
    public SystemMessage take(SystemMessageType type) throws InterruptedException {
    	if (type == SystemMessageType.CONSENSUS) {
    		return consMsgQueue.take();
    	} else if (type == SystemMessageType.HEART) {
    		return heartMsgQueue.take();
    	} else if (type == SystemMessageType.LC) {
    		return lcMsgQueue.take();
    	}
    	return null;
    }
}
