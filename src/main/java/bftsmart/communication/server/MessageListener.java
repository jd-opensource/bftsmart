package bftsmart.communication.server;

import bftsmart.communication.SystemMessage;

public interface MessageListener {

	void onReceived(SystemMessage message);

}
