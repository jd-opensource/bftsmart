package bftsmart.communication;

public interface MessageCodec<T> {
	
	
	byte[] encode(T message);
	
	
	T decode(byte[] bytes) throws MessageAuthenticationException, IllegalMessageException;
	
}
