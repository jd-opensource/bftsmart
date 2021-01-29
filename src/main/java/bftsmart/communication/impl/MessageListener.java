package bftsmart.communication.impl;

import bftsmart.communication.SystemMessage;

public interface MessageListener {

	void onReceived(SystemMessage message);

}
