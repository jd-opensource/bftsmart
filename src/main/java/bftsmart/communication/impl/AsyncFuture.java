package bftsmart.communication.impl;

/**
 * 提供对异步操作的结果描述；
 * 
 * @param <R> class
 */
public interface AsyncFuture<S, R> {

	/**
	 * 关联的上下文对象；
	 * 
	 * @return
	 */
	S getSource();

	/**
	 * 返回异步操作的结果；<br>
	 * 
	 * 注：此方法将堵塞当前线程直至异步操作完成并返回结果；
	 * 
	 * @return v
	 */
	R getReturn();

	/**
	 * 返回异步操作的结果；<br>
	 * 
	 * @param timeout 超时市场，毫秒；
	 * @return
	 */
	R getReturn(long timeout);

	/**
	 * 操作是否已完成；
	 * 
	 * 当操作成功返回或者异常返回时，都表示为已完成；
	 * 
	 * @return boolean
	 */
	boolean isDone();

	/**
	 * 操作是否发生异常；
	 * 
	 * @return boolean
	 */
	boolean isExceptionally();

	/**
	 * 错误信息；
	 * 
	 * @return
	 */
	Throwable getError();

}
