package test.bftsmart.communication.server;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import bftsmart.communication.server.AbstractStreamConnection;
import bftsmart.reconfiguration.ViewTopology;

public class MessageStreamConnection extends AbstractStreamConnection{
	
	private MessageStreamNode remoteNode;

	public MessageStreamConnection(String realmName, ViewTopology viewTopology, int remoteId,
			MessageStreamNode remoteNode) {
		super(realmName, viewTopology, remoteId, remoteNode.getMessageInQueue());
		this.remoteNode = remoteNode;
	}

	@Override
	public boolean isAlived() {
		return false;
	}

	@Override
	protected void rebuildConnection(long timeoutMillis) throws IOException {
	}

	@Override
	protected void closeConnection() {
	}

	@Override
	protected DataOutputStream getOutputStream() {
		return remoteNode.getOutputStream();
	}

	@Override
	protected DataInputStream getInputStream() {
		return remoteNode.getInputStream();
	}

}
