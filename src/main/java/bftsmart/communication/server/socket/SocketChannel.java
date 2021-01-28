package bftsmart.communication.server.socket;

import java.io.IOException;
import java.net.Socket;

import bftsmart.communication.server.IOChannel;

class SocketChannel extends IOChannel {

		private Socket socket;

		public SocketChannel(Socket socket) throws IOException {
			super(socket.getInputStream(), socket.getOutputStream());
			this.socket = socket;
		}

		@Override
		public boolean isClosed() {
			return super.isClosed() || socket == null || socket.isClosed() || (!socket.isConnected())
					|| socket.isInputShutdown() || socket.isOutputShutdown();
		}

		@Override
		protected void postClosed() {
			try {
				if (socket != null) {
					socket.close();
				}
			} catch (Exception e) {
			}
			socket = null;
		}
	}