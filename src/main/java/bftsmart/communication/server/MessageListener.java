package bftsmart.communication.server;

import bftsmart.communication.SystemMessage;
import bftsmart.communication.queue.MessageQueue.SystemMessageType;

public interface MessageListener {

	void onReceived(SystemMessageType messageType, SystemMessage message);

}
