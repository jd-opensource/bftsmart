package bftsmart.communication.server;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class AsyncFutureTask<S, R> implements AsyncFuture<S, R> {

	private CountDownLatch latch = new CountDownLatch(1);

	private R result;

	private Throwable error;

	private volatile boolean done = false;

	private S source;

	private CompletedCallback<S, R> callback;
	
	public AsyncFutureTask(S source) {
		this.source = source;
	}
	

	@Override
	public S getSource() {
		return source;
	}
	
	@Override
	public R getReturn() {
		while (true) {
			try {
				latch.await();
				break;
			} catch (InterruptedException e) {
			}
		}
		
		return result;
	}

	@Override
	public R getReturn(long timeout) {
		if (timeout < 0) {
			throw new IllegalArgumentException("The value of timeout argument is negative!");
		}
		long startTs = System.currentTimeMillis();
		long elapsedTs = 0 ;
		while (elapsedTs < timeout) {
			try {
				latch.await(timeout - elapsedTs, TimeUnit.MILLISECONDS);
				break;
			} catch (InterruptedException e) {
			}
			elapsedTs = System.currentTimeMillis() - startTs;
		}
		return result;
	}

	@Override
	public boolean isDone() {
		return done;
	}

	@Override
	public boolean isExceptionally() {
		return error != null;
	}

	@Override
	public Throwable getError() {
		return error;
	}

	public synchronized void complete(R result) {
		if (done) {
			return;
		}
		this.result = result;
		latch.countDown();
		done = true;
		
		doCallback();
	}
	
	
	
	public synchronized void error(Throwable error) {
		if (done) {
			return;
		}
		this.error = error;
		latch.countDown();
		done = true;
		
		doCallback();
	}

	
	private void doCallback() {
		if (callback != null) {
			callback.onCompleted(source, result, error);
		}
	}
	
	/**
	 * 设置回调；<p>
	 * 
	 * 当操作完成时回调方法；<p>
	 * 
	 * 回调操作
	 * 
	 * @param callback
	 * @return 返回当前的 {@link AsyncFuture} 实例；
	 */
	AsyncFuture<S, R> setCallback(CompletedCallback<S, R> callback){
		if (this.callback != null) {
			throw new IllegalArgumentException("The callback has been setted!");
		}
		this.callback  = callback;
		return this;
	}
}
