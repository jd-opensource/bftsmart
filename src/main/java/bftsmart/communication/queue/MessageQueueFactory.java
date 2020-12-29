package bftsmart.communication.queue;

import bftsmart.communication.SystemMessage;
import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.tom.leaderchange.HeartBeatMessage;
import bftsmart.tom.leaderchange.LeaderRequestMessage;
import bftsmart.tom.leaderchange.LeaderResponseMessage;
import bftsmart.tom.leaderchange.LeaderStatusRequestMessage;
import bftsmart.tom.leaderchange.LeaderStatusResponseMessage;

public class MessageQueueFactory {

    /**
     * 创建新的消息队列
     *
     * @param type
     *         消息队列类型
     * @return
     */
    public static MessageQueue newMessageQueue(MessageQueue.QueueDirection type) {
        return newMessageQueue(type, Integer.MAX_VALUE);
    }

    /**
     * 新的消息队列
     *
     * @param type
     *         队列类型
     *
     * @param capacity
     *         队列容量
     * @return
     */
    public static MessageQueue newMessageQueue(MessageQueue.QueueDirection type, int capacity) {
        /**
         * 暂时只支持接收Socket消息队列
         */
        if (type == MessageQueue.QueueDirection.IN) {
            return new MessageInQueue(capacity);
        } else {
            throw new IllegalArgumentException("Factory can create in queue only !!!");
        }
    }
    
}
