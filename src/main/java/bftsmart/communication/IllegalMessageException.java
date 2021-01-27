package bftsmart.communication;

/**
 * 不合法的消息格式错误；
 * <p>
 * 
 * @author huanghaiquan
 *
 */
public class IllegalMessageException extends MessageException {

	private static final long serialVersionUID = 5898942127123704248L;

	public IllegalMessageException(String message) {
		super(message);
	}

	public IllegalMessageException(String message, Throwable cause) {
		super(message, cause);
	}

}
