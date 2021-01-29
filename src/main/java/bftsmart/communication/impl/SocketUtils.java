package bftsmart.communication.impl;

import java.net.Socket;
import java.net.SocketException;

public class SocketUtils {

	public static void setSocketOptions(Socket socket) throws SocketException {
		socket.setTcpNoDelay(true);
	}
	
}
