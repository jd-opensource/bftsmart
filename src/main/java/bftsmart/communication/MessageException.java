package bftsmart.communication;

/**
 * 消息错误；
 * <p>
 * 
 * @author huanghaiquan
 *
 */
public class MessageException extends Exception {

	private static final long serialVersionUID = 5898942127123704248L;

	public MessageException(String message) {
		super(message);
	}

	public MessageException(String message, Throwable cause) {
		super(message, cause);
	}

}
