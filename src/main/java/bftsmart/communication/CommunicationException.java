package bftsmart.communication;

public class CommunicationException extends RuntimeException{
	
	private static final long serialVersionUID = 5898942127123704248L;

	
	public CommunicationException(String message) {
		super(message);
	}
	
	public CommunicationException(String message, Throwable cause) {
		super(message, cause);
	}

}
