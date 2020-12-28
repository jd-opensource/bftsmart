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
    public static MessageQueue newMessageQueue(MessageQueue.QUEUE_TYPE type) {
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
    public static MessageQueue newMessageQueue(MessageQueue.QUEUE_TYPE type, int capacity) {
        /**
         * 暂时只支持接收Socket消息队列
         */
        if (type == MessageQueue.QUEUE_TYPE.IN) {
            return new MessageInQueue(capacity);
        } else {
            throw new IllegalArgumentException("Factory can create in queue only !!!");
        }
    }


    /**
     * 返回消息类型枚举
     *
     * @param sm
     *         消息
     * @return
     *         枚举类型
     */
    public static MessageQueue.MSG_TYPE msgType(SystemMessage sm) {
        if (sm instanceof ConsensusMessage) {
            return MessageQueue.MSG_TYPE.CONSENSUS;
        } else if (sm instanceof HeartBeatMessage 
        		|| sm instanceof LeaderRequestMessage 
        		|| sm instanceof LeaderResponseMessage
        		|| sm instanceof LeaderStatusRequestMessage
        		|| sm instanceof LeaderStatusResponseMessage) {
            return MessageQueue.MSG_TYPE.HEART;
        } else {
            return MessageQueue.MSG_TYPE.LC;
        }
    }
}
