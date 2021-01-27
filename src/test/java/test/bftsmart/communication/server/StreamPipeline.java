package test.bftsmart.communication.server;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.bouncycastle.util.Arrays;

import utils.io.BytesUtils;

/**
 * 流管道；
 * 
 * <p>
 * 
 * 提供了基于流的字节数组先进先出队列；
 * 
 * @author huanghaiquan
 *
 */
public class StreamPipeline {

	private Object mutex = new Object();
	private BlockingQueue<byte[]> bufferQueue;

	private Output out = new Output();
	private Input in = new Input();
	
	private DataOutputStream dataOut = new DataOutputStream(out);
	private DataInputStream dataIn = new DataInputStream(in);

	public StreamPipeline() {
		this(1000);
	}

	public StreamPipeline(int capacity) {
		this(new LinkedBlockingQueue<>(capacity));
	}
	
	public StreamPipeline(BlockingQueue<byte[]> queue) {
		bufferQueue = queue;
	}
	
	public void write(byte[] bytes) {
		try {
			getOutputStream().write(bytes);
		} catch (IOException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}
	
	public void write(byte[] bytes, int off, int len) {
		try {
			getOutputStream().write(bytes, off, len);
		} catch (IOException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}
	
	public DataOutputStream getOutputStream() {
		return dataOut;
	}

	public DataInputStream getInputStream() {
		return dataIn;
	}
	
	private byte[] take() throws InterruptedException {
			return bufferQueue.take();
	}
	
	private void put(byte[] data) throws InterruptedException {
		synchronized (mutex) {
			bufferQueue.put(data);
		}
	}

	private class Output extends OutputStream {

		@Override
		public void write(int b) throws IOException {
			do {
				try {
					put(new byte[] { (byte) b });
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
					put(Arrays.copyOfRange(b, off, off + len));
					return;
				} catch (InterruptedException e) {
				}
			} while (true);
		}
	}

	private class Input extends InputStream {

		private volatile ByteArrayInputStream in = new ByteArrayInputStream(BytesUtils.EMPTY_BYTES);

		private void fillBuffer() {
			do {
				try {
					byte[] bytes = take();
					in = new ByteArrayInputStream(bytes);
					return;
				} catch (InterruptedException e) {
				}
			} while (true);
		}

		@Override
		public int read() throws IOException {
			do {
				int v = in.read();
				if (v > -1) {
					return v;
				}
				fillBuffer();
			} while (true);
		}

		@Override
		public int read(byte[] b) throws IOException {
			return read(b, 0, b.length);
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
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
