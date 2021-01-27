package bftsmart.communication;

/**
 * 消息认证错误；
 * <p>
 * 
 * @author huanghaiquan
 *
 */
public class MessageAuthenticationException extends MessageException {

	private static final long serialVersionUID = 5898942127123704248L;

	public MessageAuthenticationException(String message) {
		super(message);
	}

	public MessageAuthenticationException(String message, Throwable cause) {
		super(message, cause);
	}

}
