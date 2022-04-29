/**
Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package bftsmart.communication.client.netty;

import bftsmart.communication.client.ClientCommunicationServerSide;
import bftsmart.communication.client.RequestReceiver;
import bftsmart.reconfiguration.ViewTopology;
import bftsmart.tom.ReplicaConfiguration;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.util.TOMUtil;
import bftsmart.util.SSLContextFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslHandler;
import org.slf4j.LoggerFactory;
import utils.net.SSLMode;
import utils.net.SSLSecurity;

import javax.crypto.Mac;
import javax.net.ssl.SSLEngine;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;

/**
 *
 * @author Paulo
 */
@Sharable
public class NettyClientServerCommunicationSystemServerSide extends SimpleChannelInboundHandler<TOMMessage>
		implements ClientCommunicationServerSide {

	private RequestReceiver requestReceiver;
	private Map<Integer, NettyClientServerSession> sessionTable;
	private ReentrantReadWriteLock rl;
	private ViewTopology controller;
	private boolean closed = false;
	private Channel mainChannel;

	// This locked seems to introduce a bottleneck and seems useless, but I cannot
	// recall why I added it
	// private ReentrantLock sendLock = new ReentrantLock();
	private NettyServerPipelineFactory serverPipelineFactory;
	private static final org.slf4j.Logger LOGGER = LoggerFactory
			.getLogger(NettyClientServerCommunicationSystemServerSide.class);

	public NettyClientServerCommunicationSystemServerSide(ViewTopology controller) {
		this(controller, new SSLSecurity());
	}

	public NettyClientServerCommunicationSystemServerSide(ViewTopology controller, SSLSecurity sslSecurity) {
		try {
			this.controller = controller;
			sessionTable = new ConcurrentHashMap<>();
			rl = new ReentrantReadWriteLock();
			ReplicaConfiguration staticConf = controller.getStaticConf();
			int processId = staticConf.getProcessId();

			// Configure the server.
			Mac macDummy = Mac.getInstance(staticConf.getHmacAlgorithm());

			serverPipelineFactory = new NettyServerPipelineFactory(this, sessionTable, macDummy.getMacLength(),
					controller, rl, TOMUtil.getSignatureSize(controller));

			EventLoopGroup bossGroup = new NioEventLoopGroup();

			// If the numbers of workers are not specified by the configuration file,
			// the event group is created with the default number of threads, which
			// should be twice the number of cores available.
			int nWorkers = this.controller.getStaticConf().getNumNettyWorkers();
			EventLoopGroup workerGroup = (nWorkers > 0 ? new NioEventLoopGroup(nWorkers) : new NioEventLoopGroup());

			ServerBootstrap b = new ServerBootstrap();
			b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
					.childHandler(new ServerChannelInitializer(staticConf.isSecure(processId), sslSecurity))
					.childOption(ChannelOption.SO_KEEPALIVE, true).childOption(ChannelOption.SO_REUSEADDR, true)
					.childOption(ChannelOption.TCP_NODELAY, true);
			// Bind and start to accept incoming connections.
			ChannelFuture f = b.bind(
					new InetSocketAddress(staticConf.getHost(processId), staticConf.getPort(processId))).sync();

			LOGGER.info("-- secure = {}", staticConf.isSecure(processId));
			LOGGER.info("-- ID = {}", processId);
			LOGGER.info("-- N = {}", controller.getCurrentViewN());
			LOGGER.info("-- F = {}", controller.getCurrentViewF());
			LOGGER.info("-- Port = {}", staticConf.getPort(processId));
			LOGGER.info("-- requestTimeout = {}", staticConf.getRequestTimeout());
			LOGGER.info("-- maxBatch = {}", staticConf.getMaxBatchSize());
			if (staticConf.isUseMACs())
				LOGGER.info("-- Using MACs");
			if (staticConf.isUseSignatures())
				LOGGER.info("-- Using Signatures");
			// ******* EDUARDO END **************//

			mainChannel = f.channel();

		} catch (Throwable ex) {
			LOGGER.error("[NettyClientServerCommunicationSystemServerSide] start exception!", ex);
			throw new RuntimeException("[NettyClientServerCommunicationSystemServerSide] exception.", ex);
		}
	}

	private class ServerChannelInitializer extends ChannelInitializer<SocketChannel> {

		private boolean secure;
		private SSLSecurity sslSecurity;

		public ServerChannelInitializer(boolean secure, SSLSecurity sslSecurity) {
			this.secure = secure;
			this.sslSecurity = sslSecurity;
		}

		@Override
		protected void initChannel(SocketChannel ch) throws Exception {
			if(secure) {
				SSLEngine sslEngine = SSLContextFactory.getSSLContext(false, sslSecurity).createSSLEngine();
				sslEngine.setUseClientMode(false);
				if(null != sslSecurity.getEnabledProtocols() && sslSecurity.getEnabledProtocols().length > 0) {
					sslEngine.setEnabledProtocols(sslSecurity.getEnabledProtocols());
				}
				if(null != sslSecurity.getCiphers() && sslSecurity.getCiphers().length > 0) {
					sslEngine.setEnabledCipherSuites(sslSecurity.getCiphers());
				}

				sslEngine.setNeedClientAuth(sslSecurity.getSslMode(false).equals(SSLMode.TWO_WAY));
				ch.pipeline().addFirst(new SslHandler(sslEngine));
			}
			ch.pipeline().addLast(serverPipelineFactory.getDecoder());
			ch.pipeline().addLast(serverPipelineFactory.getEncoder());
			ch.pipeline().addLast(serverPipelineFactory.getHandler());
		}
	}

	private void closeChannelAndEventLoop(Channel c) {
		c.flush();
		c.deregister();
		c.close();
		c.eventLoop().shutdownGracefully();
	}

	@Override
	public void shutdown() {
		if (closed) {
			return;
		}
		LOGGER.debug("Shutting down Netty system");

		this.closed = true;

		closeChannelAndEventLoop(mainChannel);

		rl.readLock().lock();
		ArrayList<NettyClientServerSession> sessions = new ArrayList<>(sessionTable.values());
		rl.readLock().unlock();
		for (NettyClientServerSession ncss : sessions) {

			closeChannelAndEventLoop(ncss.getChannel());

		}

		LOGGER.info("NettyClientServerCommunicationSystemServerSide is halting.");
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {

		if (this.closed) {
			closeChannelAndEventLoop(ctx.channel());
			return;
		}

		if (cause instanceof ClosedChannelException)
			LOGGER.error("Connection with client closed.");
		else if (cause instanceof ConnectException) {
			LOGGER.error("Impossible to connect to client.");
		} else {
			LOGGER.error("exceptionCaught", cause);
		}
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, TOMMessage sm) throws Exception {

		if (this.closed) {
			closeChannelAndEventLoop(ctx.channel());
			return;
		}

		// delivers message to TOMLayer
		if (requestReceiver == null)
			LOGGER.debug("RECEIVER NULO!!!!!!!!!!!!");
		else
			requestReceiver.requestReceived(sm);
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) {

		if (this.closed) {
			closeChannelAndEventLoop(ctx.channel());
			return;
		}
		LOGGER.debug("Session Created, active clients {} ", sessionTable.size());
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) {

		if (this.closed) {
			closeChannelAndEventLoop(ctx.channel());
			return;
		}

		rl.writeLock().lock();
		try {
			Set s = sessionTable.entrySet();
			Iterator i = s.iterator();
			while (i.hasNext()) {
				Entry m = (Entry) i.next();
				NettyClientServerSession value = (NettyClientServerSession) m.getValue();
				if (ctx.channel().equals(value.getChannel())) {
					int key = (Integer) m.getKey();
					LOGGER.debug("#Removing client channel with ID {} ", key);
					sessionTable.remove(key);
					LOGGER.debug("#active clients {}", sessionTable.size());
					break;
				}
			}

		} finally {
			rl.writeLock().unlock();
		}
		LOGGER.debug("Session Closed, active clients {}", sessionTable.size());
	}

	@Override
	public void setRequestReceiver(RequestReceiver tl) {
		this.requestReceiver = tl;
	}

	@Override
	public void send(int[] targets, TOMMessage sm, boolean serializeClassHeaders) {

		// serialize message
		DataOutputStream dos = null;

		byte[] data = null;
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			dos = new DataOutputStream(baos);
			sm.wExternal(dos);
			dos.flush();
			data = baos.toByteArray();
			sm.serializedMessage = data;
		} catch (IOException ex) {
			LOGGER.error("Error enconding message.");
		} finally {
			try {
				dos.close();
			} catch (IOException ex) {
				LOGGER.error("Exception closing DataOutputStream: {}", ex.getMessage());
			}
		}

		// replies are not signed in the current JBP version
		sm.signed = false;
		// produce signature if necessary (never in the current version)
		if (sm.signed) {
			// ******* EDUARDO BEGIN **************//
			byte[] data2 = TOMUtil.signMessage(controller.getStaticConf().getRSAPrivateKey(), data);
			// ******* EDUARDO END **************//
			sm.serializedMessageSignature = data2;
		}

		for (int i = 0; i < targets.length; i++) {
			rl.readLock().lock();
			// sendLock.lock();
			try {
				NettyClientServerSession ncss = (NettyClientServerSession) sessionTable.get(targets[i]);
				if (ncss != null) {
					Channel session = ncss.getChannel();
					sm.destination = targets[i];
					// send message
					session.writeAndFlush(sm); // This used to invoke "await". Removed to avoid blockage and race
												// condition.

					/////// TODO: replace this patch for a proper client preamble
				} else if (sm.getSequence() >= 0 && sm.getSequence() <= 5) {

					final int id = targets[i];
					final TOMMessage msg = sm;

					Thread t = new Thread() {

						public void run() {

							LOGGER.debug(
									"Received request from {} before establishing Netty connection. Re-trying until connection is established",
									id);

							NettyClientServerSession ncss = null;
							while (ncss == null) {

								rl.readLock().lock();

								try {
									Thread.sleep(1000);
								} catch (InterruptedException ex) {
									java.util.logging.Logger
											.getLogger(NettyClientServerCommunicationSystemServerSide.class.getName())
											.log(Level.SEVERE, null, ex);
								}

								ncss = (NettyClientServerSession) sessionTable.get(id);
								if (ncss != null) {
									Channel session = ncss.getChannel();
									msg.destination = id;
									// send message
									session.writeAndFlush(msg);
								}

								rl.readLock().unlock();

							}

							LOGGER.debug("Connection with {} established", id);

						}

					};

					t.start();
					///////////////////////////////////////////
				} else {
					LOGGER.debug("!!!!!!!!NettyClientServerSession NULL !!!!!! sequence: {}, ID: {}", sm.getSequence(),
							targets[i]);
				}
			} finally {
				// sendLock.unlock();
				rl.readLock().unlock();
			}
		}
	}

	@Override
	public int[] getClients() {

		rl.readLock().lock();
		Set s = sessionTable.keySet();
		int[] clients = new int[s.size()];
		Iterator it = s.iterator();
		int i = 0;
		while (it.hasNext()) {

			clients[i] = ((Integer) it.next()).intValue();
			i++;
		}

		rl.readLock().unlock();

		return clients;
	}

}
