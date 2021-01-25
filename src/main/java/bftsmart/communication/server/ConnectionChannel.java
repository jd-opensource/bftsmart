package bftsmart.communication.server;

public interface ConnectionChannel {

	
	
	OutputChannel output();
	
	InputChannel input();
	
	
	void close();
	
	
	public static interface OutputChannel{
		
		
	}
	
	public static interface InputChannel{
		
	}
	
}
