package test.bftsmart.leaderchange;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import bftsmart.communication.SystemMessage;
import bftsmart.reconfiguration.util.HostsConfig;
import bftsmart.reconfiguration.views.NodeNetwork;
import bftsmart.reconfiguration.views.View;
import bftsmart.tom.leaderchange.LCType;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import bftsmart.communication.CommunicationLayer;
import bftsmart.communication.MessageHandler;
import bftsmart.communication.ServerCommunicationSystemImpl;
import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.consensus.messages.MessageFactory;
import bftsmart.reconfiguration.util.TOMConfiguration;
import bftsmart.reconfiguration.views.MemoryBasedViewStorage;
import bftsmart.reconfiguration.views.ViewStorage;
import bftsmart.tom.AsynchServiceProxy;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.core.TOMLayer;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.leaderchange.HeartBeatMessage;
import bftsmart.tom.leaderchange.LCMessage;
import bftsmart.tom.leaderchange.LeaderResponseMessage;

/**
 * @Author: zhangshuang
 * @Date: 2020/3/18 1:57 PM Version 1.0
 */
public class ConsensusTest_ {

	private static final ExecutorService nodeStartPools = Executors.newCachedThreadPool();

	private static final ExecutorService clientThread = Executors.newFixedThreadPool(20);

	private ServiceReplica[] serviceReplicas;

	private TestNodeServer[] serverNodes;

	private List<HostsConfig.Config> configList = new ArrayList<>();

	private List<NodeNetwork> addresses = new ArrayList<>();

	private Properties systemConfig;

	private ServerCommunicationSystemImpl[] serverCommunicationSystems;

	private static int clientProcId = 11000;

	private AsynchServiceProxy clientProxy;

	private View latestView;

	private int nodeNum = 4;

	private byte[] bytes;

	private int msgNum = 2;

	private String realmName = new Random().toString() + System.currentTimeMillis();


	/**
	 * 准备共识客户端
	 */
	public void createClient(int nodeNum) {
		TOMConfiguration config = loadClientConfig(nodeNum);
		latestView = new View(0, config.getInitialView(), config.getF(), addresses.toArray(new NodeNetwork[addresses.size()]));
		ViewStorage viewStorage = new MemoryBasedViewStorage(latestView);
		clientProxy = new AsynchServiceProxy(config, viewStorage);
		Random random = new Random();
		bytes = new byte[4];
		random.nextBytes(bytes);

	}

	private TOMConfiguration loadClientConfig(int nodeNum) {
		try {
			return new TOMConfiguration(clientProcId, generateSystemFile(nodeNum), generateHostsConfig(nodeNum));
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Client config file resolve error!");
		}
	}

	/**
	 * 用例1：四个节点的正常共识过程
	 */
	@Test
	public void test4NodeNormalConsensus() throws InterruptedException {

		createClient(4);

		initNode(4);

		// simple send msg test
		for (int i = 0; i < msgNum; i++) {
			clientThread.execute(() -> {
				System.out.println("-- client will send request --");
				clientProxy.invokeOrdered(bytes);
			});
		}

		Thread.sleep(20000);

		for (int i = 0; i < nodeNum; i++) {
			assertEquals(1, serverNodes[i].getReplica().getTomLayer().getLastExec());
		}
	}

	/**
	 * 用例2：验证重启任意非领导者节点，节点共识能否恢复正常
	 */
	@Test
	public void test4NodeStopAndRestartAnyNormalNode() throws InterruptedException {

		createClient(4);

		initNode(4);

		stopConsensusNode(1);

		Thread.sleep(4000);

		System.out.println("consensus node restart!");

		startConsensusNode(1);

		Thread.sleep(4000);

		System.out.println("-- client will send requests!--");
		// simple send msg test
		for (int i = 0; i < msgNum; i++) {
			clientThread.execute(() -> {
				clientProxy.invokeOrdered(bytes);
			});
		}

		Thread.sleep(4000);

		for (int i = 0; i < msgNum; i++) {
			clientThread.execute(() -> {
				clientProxy.invokeOrdered(bytes);
			});
		}

		Thread.sleep(50000);

		for (int i = 0; i < nodeNum; i++) {
			assertEquals(3, serverNodes[i].getReplica().getTomLayer().getLastExec());
		}

	}

	/**
	 * 用例3：所有节点依次进行LC
	 */
	@Test
	public void test4NodeLCOneByOne() throws InterruptedException {

		createClient(4);

		initNode(4);

		for (int i = 0; i < nodeNum; i++) {

			stopConsensusNode(i);

			Thread.sleep(40000);

			startConsensusNode(i);

			Thread.sleep(40000);

		}
		assertEquals(4, serverNodes[0].getReplica().getTomLayer().getSynchronizer().getLCManager().getLastReg());
		assertEquals(0, serverNodes[0].getReplica().getTomLayer().getSynchronizer().getLCManager().getCurrentLeader());
	}

	/**
	 * 用例4：共识节点仅2f个存活，且存在领导者，在这种环境中产生交易导致共识僵持，并且在200秒内启动新的共识节点使节点总数满足>=2f+1，最终共识恢复正常状态
	 */
	@Test
	public void test4NodeConsensusBlockRecoveryIn200s() throws InterruptedException {

		createClient(4);

		initNode(4);

		// 停掉节点2，3
		stopConsensusNode(2);

		stopConsensusNode(3);

		System.out.println("start send tx!");
		// 发送交易导致共识僵持
		clientThread.execute(() -> {
			clientProxy.invokeOrdered(bytes);
		});

		Thread.sleep(110000);

		System.out.println("restart node 2 and 3 !");

		startConsensusNode(2);

		startConsensusNode(3);

		Thread.sleep(30000);

		clientThread.execute(() -> {
			clientProxy.invokeOrdered(bytes);
		});

		try {
			System.out.println("----------------- Succ Completed -----------------");
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 用例5：取消2，3节点对共识消息的处理，让0触发交易处理超时，验证能否正常进行LC过程,以及后续的共识能否正常进行
	 */
	@Test
	public void test4NodeConsensusBlockRecovery() throws InterruptedException {

		createClient(4);

		initNode(4);

		// 通过mock修改共识节点编号为2,3的节点的MessageHandler处理
		mockMessageHandlerTest4Nodes(2);

		mockMessageHandlerTest4Nodes(3);

		Thread.sleep(10000);
		System.out.println("start send tx!");
		// 发送交易导致共识僵持
		clientThread.execute(() -> {
			clientProxy.invokeOrdered(bytes);
		});

		Thread.sleep(450000);

		// 验证后续交易是否正常共识
		for (int i = 0; i < 10; i++) {
			// 发送交易导致共识僵持
			clientThread.execute(() -> {
				clientProxy.invokeOrdered(bytes);
			});
		}
		try {
			System.out.println("----------------- Succ Completed -----------------");
			Thread.sleep(Integer.MAX_VALUE);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 用例6：N = 7的场景，停掉其中两个节点（0，6）包括领导者从而触发LC，通过Mock把剩下5个节点构造成两个网络分区，其中一个分区中的三个节点处于发送stopdata状态，
	 * 重新启动停掉的两个节点
	 * 构造步骤：
	 * 1.启动7个节点；
	 * 2.停节点0，6，其中0是领导者；
	 * 3.节点1，2，3，4，5因为领导者超时触发LC；
	 * 4.当LC 运行到同步阶段，通过mock处理，让1，5节点处于收发不到Sync，以及后续任何消息的状态；遗留2，3，4节点处于提交选举而因为没有收到SYNC消息无法恢复到正常状态；
	 * 5.重新启动节点0, 6
	 * 6.验证未完成的LC能否继续完成，交易能否正常进行共识
	 */
	@Test
	public void test7NodeLCBlockRecovery() throws InterruptedException {

		createClient(7);
		initNode(7);

	    mockCsNewLeader1(1);
	    mockCsNormalNode5(5);

	    mockMessageHandlerNewLeader1(1);
	    mockMessageHandlerNormalNode5(5);

		// 停掉节点0，6
		stopConsensusNode(0);

		stopConsensusNode(6);

		Thread.sleep(70000);

		startConsensusNode(0);

		startConsensusNode(6);

		Thread.sleep(10000);

		System.out.println("start send tx!");
		clientThread.execute(() -> {
			clientProxy.invokeOrdered(bytes);
		});

		try {
			System.out.println("----------------- Succ Completed -----------------");
			Thread.sleep(Integer.MAX_VALUE);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}


	/**
	 * 用例7：N = 7的场景，停掉其中两个节点（0，6）包括领导者从而触发LC，通过Mock把剩下5个节点构造成两个网络分区，其中一个分区中的三个节点处于发送stopdata状态，
	 * 重新启动停掉的两个节点
	 * 构造步骤：
	 * 1.启动7个节点；
	 * 2.停节点0，6，其中0是领导者；
	 * 3.节点1，2，3，4，5因为领导者超时触发LC；
	 * 4.当LC 运行到同步阶段，通过mock处理，让1，5节点处于收发不到Sync，以及后续任何消息的状态；遗留2，3，4节点处于提交选举而因为没有收到SYNC消息无法恢复到正常状态；
	 * 5.重新启动节点0， 使0处于选举进行中，并触发领导者确认任务超时，发送新一轮摄政期的Stop消息；
	 * 6.停止2，3，4节点对旧摄政期的STOP消息定时器；
	 * 7.重新启动节点6，6会触发领导者确认任务超时，发送新一轮摄政期的Stop消息；
	 * 8.目前0，2，3，4，6  5个节点处于同一个网络分区中，且0节点处于选举中，对于新摄政期的消息放入超期缓存，但最终满足条件时可以完成新摄政期的LC过程；
	 */
	@Test
	public void test7NodeLCBlockRecoveryFromInSelecting() throws InterruptedException {

		// 启动7个节点；
		createClient(7);
		initNode(7);

		// 构造Mock类；
		mockCsNewLeader1(1);
		mockCsNormalNode5(5);

		mockMessageHandlerNewLeader1(1);
		mockMessageHandlerNormalNode5(5);

		// 停掉节点0，6
		stopConsensusNode(0);

		stopConsensusNode(6);

		Thread.sleep(70000);

		System.out.println("start consensus node 0!");

		startConsensusNode(0);

		Thread.sleep(50000);

		// 停掉节点2，3，4的Stop定时器
		serverNodes[2].getReplica().getTomLayer().requestsTimer.stopAllSTOPs();

		serverNodes[3].getReplica().getTomLayer().requestsTimer.stopAllSTOPs();

		serverNodes[4].getReplica().getTomLayer().requestsTimer.stopAllSTOPs();

		Thread.sleep(20000);

		System.out.println("start consensus node 6!");

		startConsensusNode(6);

		Thread.sleep(20000);

		System.out.println("start send tx!");
//		验证后续交易是否正常共识
		for (int i = 0; i < 10; i++) {
			// 发送交易导致共识僵持
			clientThread.execute(() -> {
				clientProxy.invokeOrdered(bytes);
			});
		}

		try {
			System.out.println("----------------- Succ Completed -----------------");
			Thread.sleep(Integer.MAX_VALUE);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	/**
	 * 用例8：N = 7的场景，停掉其中两个节点（0，6）包括领导者从而触发LC，通过Mock让新领导者发送的SYNC消息丢失，
	 * 导致5个节点中四个处于提交选举状态，领导者虽然丢失了SYNC消息但心跳正常，不会触发新一轮的LC；但共识没有恢复，通过交易超时触发新一轮LC
	 * 构造步骤：
	 * 1.启动7个节点；
	 * 2.通过MOCK构造新领导者丢失SYNC消息，其他消息正常
	 * 3.停节点0，6，其中0是领导者；
	 * 4.节点1，2，3，4，5因为领导者超时触发LC，但不能成功完成；
	 * 5.发送交易，待交易超时触发LC；验证LC，共识能否恢复；
	 * 7.重新启动0，6，验证领导者确认能否正常进行；
	 * 8.发送一批交易验证能否正常进行共识；
	 */
	@Test
	public void test7NodeMissSyncMsgLeadtoConsensusException() throws InterruptedException {
		// 启动7个节点；
		createClient(7);
		initNode(7);

		// 构造新领导者丢弃Sync消息；
		mockCsNewLeaderDiscardSync(1);

		// 停掉节点0，6
		stopConsensusNode(0);

		stopConsensusNode(6);

		Thread.sleep(50000);

		System.out.println("start send tx!");
		clientThread.execute(() -> {
			clientProxy.invokeOrdered(bytes);
		});

		Thread.sleep(400000);

		System.out.println("start send tx again!");

		for (int i = 0; i <10; i++) {
			clientThread.execute(() -> {
				clientProxy.invokeOrdered(bytes);
			});
		}

		try {
			System.out.println("----------------- Succ Completed -----------------");
			Thread.sleep(Integer.MAX_VALUE);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 用例9：N = 7的场景
	 * 1. 停6后，发送交易共识完成；
	 * 2. 1~2分钟后，停5，发送交易，共识完成；
	 * 3. 1~2分钟后，停4，发送交易，LC无法完成；
	 * 4. 1~2分钟后，停3，发送交易，LC无法完成；
	 * 5. 1~2分钟后，停2，发送交易，LC无法完成；
	 * 6. 1~2分钟后，停1，发送交易，LC无法完成；
	 * 7. 1~2分钟后，启动4，发送交易；
	 * 8. 1~2分钟后，启动3，发送交易；
	 * 9. 1~2分钟后，启动2，发送交易；
	 * 10.1~2分钟后，启动1，发送交易;
	 */
	@Test
	public void test7NodeStopStartAndTxsSend() throws InterruptedException {

		// 启动7个节点；
		createClient(7);
		initNode(7);

		System.out.println("will stop 6!");
		stopConsensusNode(6);
		Thread.sleep(2000);
		clientThread.execute(() -> {
			clientProxy.invokeOrdered(bytes);
		});

		Thread.sleep(10000);

		System.out.println("will stop 5!");
		stopConsensusNode(5);
		Thread.sleep(2000);
		clientThread.execute(() -> {
			clientProxy.invokeOrdered(bytes);
		});

		Thread.sleep(10000);

		System.out.println("will stop 4!");
		stopConsensusNode(4);
		Thread.sleep(2000);
		clientThread.execute(() -> {
			clientProxy.invokeOrdered(bytes);
		});

		Thread.sleep(100000);

		System.out.println("will stop 3!");
		stopConsensusNode(3);
		Thread.sleep(2000);
		clientThread.execute(() -> {
			clientProxy.invokeOrdered(bytes);
		});

		Thread.sleep(10000);

		System.out.println("will stop 2!");
		stopConsensusNode(2);
		Thread.sleep(2000);
		clientThread.execute(() -> {
			clientProxy.invokeOrdered(bytes);
		});

		Thread.sleep(10000);

		System.out.println("will stop 1!");
		stopConsensusNode(1);
		Thread.sleep(2000);
		clientThread.execute(() -> {
			clientProxy.invokeOrdered(bytes);
		});

		Thread.sleep(10000);

		System.out.println("will start 4!");
		startConsensusNode(4);
		Thread.sleep(2000);
		clientThread.execute(() -> {
			clientProxy.invokeOrdered(bytes);
		});

		Thread.sleep(10000);

		System.out.println("will start 3!");
		startConsensusNode(3);
		Thread.sleep(2000);
		clientThread.execute(() -> {
			clientProxy.invokeOrdered(bytes);
		});

		Thread.sleep(100000);

		System.out.println("will start 2!");
		startConsensusNode(2);
		Thread.sleep(2000);
		clientThread.execute(() -> {
			clientProxy.invokeOrdered(bytes);
		});

		Thread.sleep(100000);

		System.out.println("will start 1!");
		startConsensusNode(1);
		Thread.sleep(10000);
		clientThread.execute(() -> {
			clientProxy.invokeOrdered(bytes);
		});

		System.out.println("111111111");
		Thread.sleep(10000);
		clientThread.execute(() -> {
			clientProxy.invokeOrdered(bytes);
		});
		try {
			System.out.println("----------------- Succ Completed -----------------");
			Thread.sleep(Integer.MAX_VALUE);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}
	private void mockMessageHandlerTest4Nodes(int nodeId) {

	ServerCommunicationSystemImpl serverCommunicationSystem = (ServerCommunicationSystemImpl) serverNodes[nodeId].getReplica().getServerCommunicationSystem();

	MessageHandler mockMessageHandler = serverCommunicationSystem.getMessageHandler();

	doAnswer(new Answer() {
		@Override
		public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
			Object[] objs = invocationOnMock.getArguments();
			if (objs == null || objs.length != 1) {
				invocationOnMock.callRealMethod();
			} else {
				Object obj = objs[0];
				if (obj instanceof ConsensusMessage) {
					//对于消息不做任何处理
				} else if (obj instanceof LCMessage) {
					Mockito.reset(mockMessageHandler);
					invocationOnMock.callRealMethod();
				} else {
					invocationOnMock.callRealMethod();
				}
			}
			return null;
		}
	}).when(mockMessageHandler).processData(any());
}

	private boolean newLeaderDiscardSendMsgEnable = false;

	private void mockCsNewLeader1(int nodeId) {
		ServerCommunicationSystemImpl mockServerCommunicationSystem = (ServerCommunicationSystemImpl) serverNodes[nodeId].getReplica().getServerCommunicationSystem();

		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				Object[] objs = invocationOnMock.getArguments();
				Object obj = objs[1];

				if (newLeaderDiscardSendMsgEnable) {

				} else if ((obj instanceof LCMessage) && (((LCMessage) obj).getType().CODE == LCType.SYNC.CODE)) {
					newLeaderDiscardSendMsgEnable = true;
				} else {
					invocationOnMock.callRealMethod();
				}

				return null;
			}
		}).when(mockServerCommunicationSystem).send(any(), (SystemMessage)any());
	}

	private boolean newLeaderDiscardSyncMsgEnable = false;
	private void mockCsNewLeaderDiscardSync(int nodeId) {
		ServerCommunicationSystemImpl mockServerCommunicationSystem = (ServerCommunicationSystemImpl) serverNodes[nodeId].getReplica().getServerCommunicationSystem();

		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				Object[] objs = invocationOnMock.getArguments();
				Object obj = objs[1];

                if ((obj instanceof LCMessage) && (((LCMessage) obj).getType().CODE == LCType.SYNC.CODE) && !newLeaderDiscardSyncMsgEnable) {
					newLeaderDiscardSyncMsgEnable = true;
				} else {
					invocationOnMock.callRealMethod();
				}

				return null;
			}
		}).when(mockServerCommunicationSystem).send(any(), (SystemMessage)any());
	}

	private void mockCsNormalNode5(int nodeId) {
		ServerCommunicationSystemImpl mockServerCommunicationSystem = (ServerCommunicationSystemImpl) serverNodes[nodeId].getReplica().getServerCommunicationSystem();

		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				Object[] objs = invocationOnMock.getArguments();
				Object obj = objs[1];

				if (newLeaderDiscardSendMsgEnable) {
				} else {
					invocationOnMock.callRealMethod();
				}

				return null;
			}
		}).when(mockServerCommunicationSystem).send(any(), (SystemMessage)any());
	}

	private void mockCsOldLeader0(int nodeId) {
		ServerCommunicationSystemImpl mockServerCommunicationSystem = (ServerCommunicationSystemImpl) serverNodes[nodeId].getReplica().getServerCommunicationSystem();

		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				return null;
			}
		}).when(mockServerCommunicationSystem).send(any(), (SystemMessage)any());
	}



	private void mockMessageHandlerNewLeader1(int nodeId) {

		ServerCommunicationSystemImpl serverCommunicationSystem = (ServerCommunicationSystemImpl) serverNodes[nodeId].getReplica().getServerCommunicationSystem();

		MessageHandler mockMessageHandler = serverCommunicationSystem.getMessageHandler();

		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				Object[] objs = invocationOnMock.getArguments();
				if (newLeaderDiscardSendMsgEnable) {
					// 什么也不做
				} else {
					invocationOnMock.callRealMethod();
				}

				return null;
			}
		}).when(mockMessageHandler).processData(any());
	}

	private void mockMessageHandlerNormalNode5(int nodeId) {

		ServerCommunicationSystemImpl serverCommunicationSystem = (ServerCommunicationSystemImpl) serverNodes[nodeId].getReplica().getServerCommunicationSystem();

		MessageHandler mockMessageHandler = serverCommunicationSystem.getMessageHandler();

		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				Object[] objs = invocationOnMock.getArguments();
				if (newLeaderDiscardSendMsgEnable) {
					// 什么也不做
				} else {
					invocationOnMock.callRealMethod();
				}

				return null;
			}
		}).when(mockMessageHandler).processData(any());
	}

	private void startConsensusNode(int i) {
		System.out.println("I will restart consensus node "+i);
		TestNodeServer nodeServer = new TestNodeServer(i, latestView, systemConfig, new HostsConfig(configList.toArray(new HostsConfig.Config[configList.size()])));
		serverNodes[i] = nodeServer;
		nodeStartPools.execute(() -> {
				nodeServer.startNode(realmName);
			});
	}

	private void stopConsensusNode(int i) {
		System.out.println("I will stop consensus node, nodeid = "+i);
		serverNodes[i].getReplica().kill();
		System.out.println("stop node end , nodeid = "+i);
	}


	// 以下用例不再使用；
	/**
	 * 开始进行共识，之后领导者异常，然后领导者恢复
	 */
	@Test
	public void test4NodeButLeaderExceptionThenResume() {
		int consensusMsgNum = 10;

		initNode(nodeNum);

		for (int i = 0; i < consensusMsgNum; i++) {
			clientProxy.invokeOrdered(bytes);
		}

		try {
			// 延时，等待消息被处理完
			Thread.sleep(20000);
		} catch (Exception e) {
			e.printStackTrace();
		}

		MessageHandler mockMessageHandler = stopNode(0);

		// 重启之前领导者心跳服务
		restartLeaderHeartBeat(serviceReplicas, 0);

		System.out.printf("-- restart %s LeaderHeartBeat -- \r\n", 0);

		// 重置mock操作
		reset(mockMessageHandler);

		try {
			System.out.println("-- leader node has complete change --");
			Thread.sleep(Integer.MAX_VALUE);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * 开始进行共识，之后领导者异常，然后领导者恢复 然后再一次出现该现象
	 */
	@Test
	public void test4NodeLoopLeaderExceptionAndCannotReceiveTOMMessageThenResume() {

		initNode(nodeNum);

		Executors.newSingleThreadExecutor().execute(() -> {
			// 假设有10000笔消息
			for (int i = 0; i < 10000; i++) {
				clientProxy.invokeOrdered(bytes);
				if (i % 10 == 0) {
					try {
						Thread.sleep(1000);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		});

		try {
			// 仅仅延时
			Thread.sleep(5000);
		} catch (Exception e) {
			e.printStackTrace();
		}

		for (int i = 0; i < nodeNum; i++) {

			final int index = i;

			MockHandlers mockHandlers = stopNodeAndStopReceiveTOMMessage(index);

			MessageHandler mockMessageHandler = mockHandlers.messageHandler;

			TOMLayer mockTomlayer = mockHandlers.tomLayer;

			// 重启之前领导者心跳服务
			restartLeaderHeartBeat(serviceReplicas, index);

			System.out.printf("-- restart %s LeaderHeartBeat -- \r\n", index);

			// 重置mock操作
			reset(mockMessageHandler);
			reset(mockTomlayer);

			try {
				Thread.sleep(30000);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		try {
			System.out.println("-- leader node has complete change --");
			Thread.sleep(Integer.MAX_VALUE);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * 开始进行共识，之后领导者异常，然后领导者恢复 然后再一次出现该现象
	 */
	@Test
	public void test4NodeLoopLeaderExceptionThenResume() {

		initNode(nodeNum);

		Executors.newSingleThreadExecutor().execute(() -> {
			// 假设有10000笔消息
			for (int i = 0; i < 10000; i++) {
				clientProxy.invokeOrdered(bytes);
				if (i % 10 == 0) {
					try {
						Thread.sleep(1000);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		});

		try {
			// 仅仅延时
			Thread.sleep(5000);
		} catch (Exception e) {
			e.printStackTrace();
		}

		for (int i = 0; i < nodeNum; i++) {

			final int index = i;

			MessageHandler mockMessageHandler = stopNode(index);

			// 重启之前领导者心跳服务
			restartLeaderHeartBeat(serviceReplicas, index);

			System.out.printf("-- restart %s LeaderHeartBeat -- \r\n", index);

			// 重置mock操作
			reset(mockMessageHandler);

			try {
				Thread.sleep(30000);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		try {
			System.out.println("-- leader node has complete change --");
			Thread.sleep(Integer.MAX_VALUE);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * 开始进行共识，之后领导者异常，然后领导者恢复 然后再一次出现该现象
	 */
	@Test
	public void test4NodeBut2LeaderExceptionThenResume() {
		int consensusMsgNum = 10;

		initNode(nodeNum);

		for (int i = 0; i < consensusMsgNum; i++) {
			clientProxy.invokeOrdered(bytes);
		}

		try {
			// 延时，等待消息被处理完
			Thread.sleep(10000);
		} catch (Exception e) {
			e.printStackTrace();
		}

		for (int i = 0; i < 2; i++) {

			final int index = i;

			MessageHandler mockMessageHandler = stopNode(index);

			// 重启之前领导者心跳服务
			restartLeaderHeartBeat(serviceReplicas, index);

			System.out.printf("-- restart %s LeaderHeartBeat -- \r\n", index);

			// 重置mock操作
			reset(mockMessageHandler);

			try {
				Thread.sleep(30000);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		try {
			System.out.println("-- leader node has complete change --");
			Thread.sleep(Integer.MAX_VALUE);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * 开始一段时间正常共识，然后领导者异常循环切换
	 */
	@Test
	public void test4NodeFirstNormalConsensusThenLeaderPollException() {
		int consensusMsgNum = 10;

		initNode(nodeNum);

		// 正常共识
		for (int i = 0; i < consensusMsgNum; i++) {
			clientProxy.invokeOrdered(bytes);
		}

		// 领导者轮询异常
		for (int i = 0; i < nodeNum; i++) {

			final int index = i;

			MessageHandler mockMessageHandler = stopNode(index);

			// 重启之前领导者心跳服务
			restartLeaderHeartBeat(serviceReplicas, index);

			System.out.printf("-- restart %s LeaderHeartBeat -- \r\n", index);

			// 重置mock操作
			reset(mockMessageHandler);

			try {
				Thread.sleep(30000);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		try {
			System.out.println("-- total node has complete change --");
			Thread.sleep(Integer.MAX_VALUE);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * 正常共识，领导者轮询异常交替进行
	 */
	@Test
	public void test4NodeNormalConsensusAndLeadePollExceptionAlter() {
		int consensusMsgNum = 10;

		initNode(nodeNum);

		// 正常共识
		for (int i = 0; i < consensusMsgNum; i++) {
			clientProxy.invokeOrdered(bytes);
		}

		// 领导者轮询异常
		for (int i = 0; i < nodeNum; i++) {

			final int index = i;

			MessageHandler mockMessageHandler = stopNode(index);

			// 重启之前领导者心跳服务
			restartLeaderHeartBeat(serviceReplicas, index);

			System.out.printf("-- restart %s LeaderHeartBeat -- \r\n", index);

			// 重置mock操作
			reset(mockMessageHandler);

			try {
				Thread.sleep(30000);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		System.out.println("-- The first alter complete! --");

		// 正常共识
		for (int i = 0; i < consensusMsgNum; i++) {
			clientProxy.invokeOrdered(bytes);
		}

		// 领导者轮询异常
		for (int i = 0; i < nodeNum; i++) {

			final int index = i;

			MessageHandler mockMessageHandler = stopNode(index);

			// 重启之前领导者心跳服务
			restartLeaderHeartBeat(serviceReplicas, index);

			System.out.printf("-- restart %s LeaderHeartBeat -- \r\n", index);

			// 重置mock操作
			reset(mockMessageHandler);

			try {
				Thread.sleep(30000);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		try {
			System.out.println("-- total node has complete change --");
			Thread.sleep(Integer.MAX_VALUE);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	// 在有大批消息共识的过程中通过简单的停止领导者心跳触发领导者切换
	@Test
	public void OneTimeSimpleStopLeaderHbDuringConsensus() {
		int consensusMsgNum = 10000;

		initNode(nodeNum);

		nodeStartPools.execute(() -> {
			for (int i = 0; i < consensusMsgNum; i++) {
				clientProxy.invokeOrdered(bytes);
			}
		});

		stopLeaderHeartBeat(serviceReplicas);

		try {
			System.out.println("-- total node has complete change --");
			Thread.sleep(Integer.MAX_VALUE);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 领导者发送Propose消息之前网络异常，也就是说上一轮处理结束后，领导者异常
	 */
	@Test
	public void oneTimeLeaderChangeDuringConsensusWhenProposeUnSend() throws InterruptedException {

		int consensusMsgNum = 1000;

		initNode(nodeNum);

		nodeStartPools.execute(() -> {
			for (int i = 0; i < consensusMsgNum; i++) {
				if (i % 10 == 0) {
					try {
						Thread.sleep(10);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				clientProxy.invokeOrdered(bytes);
			}
		});

		Thread.sleep(5000);

		// 1节点进行领导者切换
		for (int i = 0; i < 1; i++) {

			final int index = i;

			MockHandlers mockHandlers = stopNodeAndStopProposeMsg(index);

			MessageHandler mockMessageHandler = mockHandlers.messageHandler;

			// 重启之前领导者心跳服务
			restartLeaderHeartBeat(serviceReplicas, index);

			System.out.printf("-- restart %s LeaderHeartBeat -- \r\n", index);

			// 重置mock操作
			reset(mockMessageHandler);
			reset(mockHandlers.serversCommunicationLayer);

			try {
				Thread.sleep(30000);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		try {
			System.out.println("-- total node has complete change --");
			Thread.sleep(Integer.MAX_VALUE);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	// 在有大批消息共识的过程中触发一次领导者异常与恢复
	@Test
	public void OneTimeLeaderChangeDuringConsensus() {
		int consensusMsgNum = 10000;

		initNode(nodeNum);

		nodeStartPools.execute(() -> {
			for (int i = 0; i < consensusMsgNum; i++) {
				clientProxy.invokeOrdered(bytes);
			}
		});

		// 1节点进行领导者切换
		for (int i = 0; i < 1; i++) {

			final int index = i;

			MessageHandler mockMessageHandler = stopNode(index);

			// 重启之前领导者心跳服务
			restartLeaderHeartBeat(serviceReplicas, index);

			System.out.printf("-- restart %s LeaderHeartBeat -- \r\n", index);

			// 重置mock操作
			reset(mockMessageHandler);

			try {
				Thread.sleep(30000);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		try {
			System.out.println("-- total node has complete change --");
			Thread.sleep(Integer.MAX_VALUE);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	// 在有大批消息共识的过程中触发两次领导者切换
	@Test
	public void TwoTimesleaderChangeDuringConsensus() {
		int consensusMsgNum = 5000;

		initNode(nodeNum);

		nodeStartPools.execute(() -> {
			for (int i = 0; i < consensusMsgNum; i++) {
				clientProxy.invokeOrdered(bytes);
			}
		});

		// 1,2节点分别进行领导者切换
		for (int i = 0; i < 2; i++) {

			final int index = i;

			MessageHandler mockMessageHandler = stopNode(index);

			// 重启之前领导者心跳服务
			restartLeaderHeartBeat(serviceReplicas, index);

			System.out.printf("-- restart %s LeaderHeartBeat -- \r\n", index);

			// 重置mock操作
			reset(mockMessageHandler);

			try {
				Thread.sleep(30000);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		try {
			System.out.println("-- total node has complete change --");
			Thread.sleep(Integer.MAX_VALUE);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

//    // client send
//    @Test
//    public void clientSend() {
//
//        int consensusNum = 2000;
//        //正常共识
//        for (int i = 0; i < consensusNum; i++ ) {
//            clientProxy.invokeOrdered(bytes);
//        }
//    }

	private MockHandlers stopNodeAndStopReceiveTOMMessage(final int index) {
		// 第一个节点持续异常
		// 重新设置leader的消息处理方式
		MessageHandler mockMessageHandler = spy(serverCommunicationSystems[index].getMessageHandler());

		// mock messageHandler对消息应答的处理
		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				Object[] objs = invocationOnMock.getArguments();
				if (objs == null || objs.length != 1) {
					invocationOnMock.callRealMethod();
				} else {
					Object obj = objs[0];
					if (obj instanceof LCMessage) {
						// 走我们设计的逻辑，即不处理
					} else if (obj instanceof LeaderResponseMessage) {
						invocationOnMock.callRealMethod();
					} else if (obj instanceof HeartBeatMessage) {
						invocationOnMock.callRealMethod();
					} else {
						invocationOnMock.callRealMethod();
					}
				}
				return null;
			}
		}).when(mockMessageHandler).processData(any());

		serverCommunicationSystems[index].setMessageHandler(mockMessageHandler);

		// spy 客户端发送的消息
		TOMLayer mockTomlayer = spy(serviceReplicas[index].getTomLayer());

		// 重新设置tomlayer处理客户端消息逻辑
		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {

				Object[] objs = invocationOnMock.getArguments();

				Object obj = objs[0];

				if (obj instanceof TOMMessage) {

					TOMMessage msg = (TOMMessage) obj;
//                    System.out.printf("I am [%s] receive tommessage -> %s \r\n", index, msg);
				}
				return null;
			}
		}).when(mockTomlayer).requestReceived(any());

		// 重设receiver
		serverCommunicationSystems[index].getClientCommunication().setRequestReceiver(mockTomlayer);

		// 领导者心跳停止
		stopLeaderHeartBeat(serviceReplicas);

		System.out.printf("-- stop %s LeaderHeartBeat -- \r\n", index);

		try {
			// 休眠40s，等待领导者切换完成
			Thread.sleep(40000);
		} catch (Exception e) {
			e.printStackTrace();
		}

		MockHandlers mockHandlers = new MockHandlers();

		mockHandlers.setMessageHandler(mockMessageHandler);
		mockHandlers.setTomLayer(mockTomlayer);

		return mockHandlers;
	}

	private MockHandlers stopNodeAndStopProposeMsg(final int index) {
		// 第一个节点持续异常
		// 重新设置leader的消息处理方式
		MessageHandler mockMessageHandler = spy(serverCommunicationSystems[index].getMessageHandler());

		// mock messageHandler对消息应答的处理
		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				Object[] objs = invocationOnMock.getArguments();
				if (objs == null || objs.length != 1) {
					invocationOnMock.callRealMethod();
				} else {
					Object obj = objs[0];
					if (obj instanceof LCMessage || obj instanceof ConsensusMessage) {
						// 走我们设计的逻辑，即不处理
					} else if (obj instanceof LeaderResponseMessage) {
						invocationOnMock.callRealMethod();
					} else if (obj instanceof HeartBeatMessage) {
						invocationOnMock.callRealMethod();
					} else {
						invocationOnMock.callRealMethod();
					}
				}
				return null;
			}
		}).when(mockMessageHandler).processData(any());

		serverCommunicationSystems[index].setMessageHandler(mockMessageHandler);

		CommunicationLayer serversCommunicationLayer = spy(
				serverCommunicationSystems[index].getServersCommunication());

		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				Object[] objs = invocationOnMock.getArguments();
				if (objs == null || objs.length != 3) {
					invocationOnMock.callRealMethod();
				} else {
					Object obj = objs[1];
					if (obj instanceof ConsensusMessage) {
						//
						ConsensusMessage cmsg = (ConsensusMessage) obj;
						if (cmsg.getType() == MessageFactory.PROPOSE) {
							// 不处理
						} else {
							invocationOnMock.callRealMethod();
						}
					} else {
						invocationOnMock.callRealMethod();
					}
				}

				return null;
			}
		}).when(serversCommunicationLayer).send(any(), any(), anyBoolean());
//TODO:
//		serverCommunicationSystems[index].setServersCommunication(serversCommunicationLayer);

		// 领导者心跳停止
		stopLeaderHeartBeat(serviceReplicas);

		System.out.printf("-- stop %s LeaderHeartBeat -- \r\n", index);

		try {
			// 休眠40s，等待领导者切换完成
			Thread.sleep(40000);
		} catch (Exception e) {
			e.printStackTrace();
		}

		MockHandlers mockHandlers = new MockHandlers();

		mockHandlers.setMessageHandler(mockMessageHandler);
		mockHandlers.setServersCommunicationLayer(serversCommunicationLayer);

		return mockHandlers;
	}

	private MessageHandler stopNode(final int index) {
		// 第一个节点持续异常
		// 重新设置leader的消息处理方式
		MessageHandler mockMessageHandler = spy(serverCommunicationSystems[index].getMessageHandler());

		// mock messageHandler对消息应答的处理
		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				Object[] objs = invocationOnMock.getArguments();
				if (objs == null || objs.length != 1) {
					invocationOnMock.callRealMethod();
				} else {
					Object obj = objs[0];
					if (obj instanceof LCMessage) {
						// 走我们设计的逻辑，即不处理
					} else if (obj instanceof LeaderResponseMessage) {
						invocationOnMock.callRealMethod();
					} else if (obj instanceof HeartBeatMessage) {
						invocationOnMock.callRealMethod();
					} else {
						invocationOnMock.callRealMethod();
					}
				}
				return null;
			}
		}).when(mockMessageHandler).processData(any());

		serverCommunicationSystems[index].setMessageHandler(mockMessageHandler);

		// 领导者心跳停止
		stopLeaderHeartBeat(serviceReplicas);

		System.out.printf("-- stop %s LeaderHeartBeat -- \r\n", index);

		try {
			// 休眠40s，等待领导者切换完成
			Thread.sleep(40000);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return mockMessageHandler;
	}

	private void initNode(int nodeSize) {

		CountDownLatch servers = new CountDownLatch(nodeSize);

		serverNodes = new TestNodeServer[nodeSize];

		// start nodeSize node servers
		for (int i = 0; i < nodeSize; i++) {
			serverNodes[i] = new TestNodeServer(i, latestView, systemConfig, new HostsConfig(configList.toArray(new HostsConfig.Config[configList.size()])));
			TestNodeServer node = serverNodes[i];
			nodeStartPools.execute(() -> {
				node.startNode(realmName);
				servers.countDown();
			});
		}

		try {
			servers.await();

			Thread.sleep(40000);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	private HostsConfig generateHostsConfig(int nodeSize) {
		try {
			String path = ConsensusTest_.class.getResource("/").toURI().getPath();
			String dirPath = new File(path).getParentFile().getParentFile().getPath() + File.separator + "config";

			FileReader fr = new FileReader(dirPath + File.separator + "hosts.config");

			BufferedReader rd = new BufferedReader(fr);
			String line = null;
			int i = 0;
			while (((line = rd.readLine()) != null) && i < nodeSize ) {

				if (!line.startsWith("#")) {
					StringTokenizer str = new StringTokenizer(line, " ");
					if (str.countTokens() > 2) {
						int id = Integer.valueOf(str.nextToken());
						String host = str.nextToken();
						int consensusPort = Integer.valueOf(str.nextToken());
						i++;
						try {
							int monitorPort = Integer.valueOf(str.nextToken());
							configList.add(new HostsConfig.Config(id, host, consensusPort, monitorPort));
							addresses.add(new NodeNetwork(host, consensusPort, monitorPort, false, false));
						} catch (Exception e) {
							configList.add(id, new HostsConfig.Config(id, host, consensusPort, -1));
							addresses.add(new NodeNetwork(host, consensusPort, -1, false, false));
						}
					}
				}
			}
			fr.close();
			rd.close();
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}

	    return new HostsConfig(configList.toArray(new HostsConfig.Config[configList.size()]));
	}
	private Properties generateSystemFile(int nodeSize) {

		// 首先删除view，然后修改配置文件
		try {

			String path = this.getClass().getResource("/").getPath();
			System.out.println("path = "+path);
			String dirPath = new File(path).getParentFile().getParentFile().getPath() + File.separator + "config";
			// 删除view
//			new File(dirPath + File.separator + "currentView").delete();
			// 删除system文件
			new File(dirPath + File.separator + "system.config").delete();

			// 根据nodeSize，重新copy一份system.config文件
			File needSystemConfig = new File(dirPath + File.separator + "system_" + nodeSize + ".config");

			// copy一份system.config
			FileUtils.copyFile(needSystemConfig, new File(dirPath + File.separator + "system.config"));

			systemConfig = utils.io.FileUtils.readProperties(new FileInputStream(dirPath + File.separator + "system.config"));

			return utils.io.FileUtils.readProperties(new FileInputStream(dirPath + File.separator + "system.config"));

		} catch (Exception e) {

			e.printStackTrace();
			return null;

		}
	}

	private void stopLeaderHeartBeat(ServiceReplica[] serviceReplicas) {

		int leadId = serviceReplicas[0].getTomLayer().getExecManager().getCurrentLeader();

		serviceReplicas[leadId].getTomLayer().heartBeatTimer.stopAll();
	}

	private void restartLeaderHeartBeat(ServiceReplica[] serviceReplicas, int node) {

		int leadId = serviceReplicas[node].getTomLayer().getExecManager().getCurrentLeader();

		System.out.printf("my new leader = %s \r\n", leadId);

		serviceReplicas[leadId].getTomLayer().heartBeatTimer.restart();
	}

	private static class MockHandlers {

		private MessageHandler messageHandler;

		private TOMLayer tomLayer;

		private CommunicationLayer serversCommunicationLayer;

		public void setMessageHandler(MessageHandler messageHandler) {
			this.messageHandler = messageHandler;
		}

		public void setTomLayer(TOMLayer tomLayer) {
			this.tomLayer = tomLayer;
		}

		public void setServersCommunicationLayer(CommunicationLayer serversCommunicationLayer) {
			this.serversCommunicationLayer = serversCommunicationLayer;
		}
	}
}
