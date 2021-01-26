package test.bftsmart.communication.server;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.LinkedBlockingQueue;

import org.bouncycastle.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.communication.queue.MessageQueue;
import bftsmart.reconfiguration.ViewTopology;
import utils.io.BytesUtils;

/**
 * 基于流的消息连接；
 * 
 * @author huanghaiquan
 *
 */
public class MessageStreamNode {

	private static final Logger LOGGER = LoggerFactory.getLogger(MessageStreamNode.class);

	private Output out = new Output();
	private Input in = new Input();

	private DataOutputStream dataout = new DataOutputStream(out);
	private DataInputStream datain = new DataInputStream(in);

	private LinkedBlockingQueue<byte[]> dataQueues = new LinkedBlockingQueue<>(1000);
	
	private final int id;
	
	private MessageQueue messageInQueue;

	public MessageStreamNode(String realmName, ViewTopology viewTopology, MessageQueue messageInQueue) {
		this.id = viewTopology.getCurrentProcessId();
		this.messageInQueue = messageInQueue;
	}

	protected DataOutputStream getOutputStream() {
		return dataout;
	}

	protected DataInputStream getInputStream() {
		return datain;
	}


	public int getId() {
		return id;
	}


	public MessageQueue getMessageInQueue() {
		return messageInQueue;
	}

	private class Output extends OutputStream {

		@Override
		public void write(int b) throws IOException {
			do {
				try {
					dataQueues.put(new byte[] { (byte) b });
					return;
				} catch (InterruptedException e) {
				}
			} while (true);
		}

		@Override
		public void write(byte[] b) throws IOException {
			write(b, 0, b.length);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			if (b == null || len <= 0) {
				return;
			}
			do {
				try {
					dataQueues.put(Arrays.copyOfRange(b, off, off + len));
					return;
				} catch (InterruptedException e) {
				}
			} while (true);
		}
	}

	private class Input extends InputStream {

		private volatile ByteArrayInputStream in = new ByteArrayInputStream(BytesUtils.EMPTY_BYTES);

		private synchronized void fillBuffer() {
			do {
				try {
					byte[] bytes = dataQueues.take();
					in = new ByteArrayInputStream(bytes);
					return;
				} catch (InterruptedException e) {
				}
			} while (true);
		}

		@Override
		public synchronized int read() throws IOException {
			do {
				int v = in.read();
				if (v > -1) {
					return v;
				}
				fillBuffer();
			} while (true);
		}

		@Override
		public synchronized int read(byte[] b) throws IOException {
			return read(b, 0, b.length);
		}

		@Override
		public synchronized int read(byte[] b, int off, int len) throws IOException {
			do {
				int r = in.read(b, off, len);
				if (r > 0) {
					return r;
				}
				fillBuffer();
			} while (true);
		}

	}

}
