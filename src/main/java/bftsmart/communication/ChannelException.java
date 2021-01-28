package bftsmart.communication;

/**
 * 消息错误；
 * <p>
 * 
 * @author huanghaiquan
 *
 */
public class ChannelException extends Exception {

	private static final long serialVersionUID = 5898942127123704248L;

	public ChannelException(String message) {
		super(message);
	}

	public ChannelException(String message, Throwable cause) {
		super(message, cause);
	}

}
