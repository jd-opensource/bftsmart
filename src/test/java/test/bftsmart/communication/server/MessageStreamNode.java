package test.bftsmart.communication.server;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.crypto.SecretKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.communication.SystemMessage;
import bftsmart.communication.queue.MessageQueue;
import bftsmart.communication.server.AbstractStreamConnection;
import bftsmart.communication.server.AsyncFuture;
import bftsmart.communication.server.AsyncFutureTask;
import bftsmart.communication.server.CompletedCallback;
import bftsmart.communication.server.MessageConnection;
import bftsmart.reconfiguration.ViewTopology;
import utils.io.BytesOutputBuffer;
import utils.io.BytesUtils;

/**
 * 基于流的消息连接；
 * 
 * @author huanghaiquan
 *
 */
public class MessageStreamNode extends AbstractStreamConnection {

	private static final Logger LOGGER = LoggerFactory.getLogger(MessageStreamNode.class);

	private volatile BytesOutputBuffer bytesBuffer = new BytesOutputBuffer();

	private Output out = new Output();
	private Input in = new Input();
	
	private DataOutputStream dataout = new DataOutputStream(out);
	private DataInputStream datain = new DataInputStream(in);

	public MessageStreamNode(String realmName, ViewTopology viewTopology, MessageQueue messageInQueue) {
		super(realmName, viewTopology, viewTopology.getCurrentProcessId(), messageInQueue);
	}

	@Override
	public boolean isAlived() {
		return true;
	}

	@Override
	protected void rebuildConnection(long timeoutMillis) throws IOException {
	}

	@Override
	protected void closeConnection() {
	}

	@Override
	protected DataOutputStream getOutputStream() {
		return dataout;
	}

	@Override
	protected DataInputStream getInputStream() {
		return datain;
	}

	private synchronized void writeBuffer(byte[] b, int off, int len) {
		bytesBuffer.writeCopy(b, off, len);
	}

	private synchronized BytesOutputBuffer drainOutput() {
		BytesOutputBuffer dataBuff = bytesBuffer;
		bytesBuffer = new BytesOutputBuffer();
		return dataBuff;
	}

	private class Output extends OutputStream {

		@Override
		public void write(int b) throws IOException {
			writeBuffer(new byte[] { (byte) b }, 0, 1);
		}

		@Override
		public void write(byte[] b) throws IOException {
			writeBuffer(b, 0, b.length);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			writeBuffer(b, off, len);
		}
	}

	private class Input extends InputStream {

		private volatile ByteArrayInputStream in = new ByteArrayInputStream(BytesUtils.EMPTY_BYTES);

		private synchronized void readBuffer() {
			BytesOutputBuffer newBuffer = drainOutput();
			byte[] bytes = newBuffer.toBytes();
			in = new ByteArrayInputStream(bytes);
		}

		@Override
		public synchronized int read() throws IOException {
			if (in.available() <= 0) {
				readBuffer();
			}
			return in.read();
		}

		@Override
		public synchronized int read(byte[] b) throws IOException {
			int r = in.read(b);
			if (r < b.length) {
				readBuffer();
				r += in.read(b, r, b.length - r);
			}
			return r;
		}

		@Override
		public synchronized int read(byte[] b, int off, int len) throws IOException {
			int r = in.read(b, off, len);
			if (r < len) {
				readBuffer();
				r += in.read(b, off + r, len - r);
			}
			return r;
		}

	}

}
