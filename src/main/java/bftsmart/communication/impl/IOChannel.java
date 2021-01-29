package bftsmart.communication.impl;

import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * IOChannel 定义了将一组输入输出流进行联动的关闭状态控制的机制；
 * 
 * @author huanghaiquan
 *
 */
public class IOChannel implements Cloneable{

	private Input in;

	private Output out;

	private volatile AtomicBoolean closed = new AtomicBoolean(false);

	public IOChannel(InputStream in, OutputStream out) {
		this.in = new Input(in);
		this.out = new Output(out);
	}

	public OutputStream getOutputStream() {
		return out;
	}

	public InputStream getInputStream() {
		return in;
	}

	public boolean isClosed() {
		return closed.get();
	}

	public void close()  {
		if (closed.get()) {
			return;
		}
		if (closed.compareAndSet(false, true)) {
			try {
				try {
					in.superClose();
				} catch (IOException e) {
				}
				try {
					out.superClose();
				} catch (IOException e) {
				}
			} finally {
				postClosed();
			}
		}
	}

	/**
	 * 关闭后触发调用；
	 * <p>
	 * 
	 * 只会触发一次；
	 */
	protected void postClosed() {
	}

	private class Input extends FilterInputStream {

		public Input(InputStream in) {
			super(in);
		}

		private void superClose() throws IOException {
			super.close();
		}

		@Override
		public void close() throws IOException {
			IOChannel.this.close();
		}
	}

	private class Output extends FilterOutputStream {

		public Output(OutputStream out) {
			super(out);
		}

		private void superClose() throws IOException {
			super.close();
		}

		@Override
		public void close() throws IOException {
			IOChannel.this.close();
		}
	}
}
