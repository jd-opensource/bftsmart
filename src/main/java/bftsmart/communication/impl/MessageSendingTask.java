package bftsmart.communication.impl;

import bftsmart.communication.SystemMessage;

public class MessageSendingTask  extends AsyncFutureTask<SystemMessage, Void> {

    public final boolean RETRY;

    public MessageSendingTask(SystemMessage message, boolean retry) {
        super(message);
        this.RETRY = retry;
    }

}