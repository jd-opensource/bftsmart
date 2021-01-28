package bftsmart.communication;

/**
 * 连接认证错误；
 * <p>
 * 
 * @author huanghaiquan
 *
 */
public class MacAuthenticationException extends Exception {

	private static final long serialVersionUID = 5898942127123704248L;

	public MacAuthenticationException(String message) {
		super(message);
	}

	public MacAuthenticationException(String message, Throwable cause) {
		super(message, cause);
	}

}
