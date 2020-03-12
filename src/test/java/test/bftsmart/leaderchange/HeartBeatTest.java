package test.bftsmart.leaderchange;

import bftsmart.communication.MessageHandler;
import bftsmart.communication.ServerCommunicationSystem;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.leaderchange.HeartBeatMessage;
import bftsmart.tom.leaderchange.HeartBeatTimer;
import bftsmart.tom.leaderchange.LCMessage;
import bftsmart.tom.leaderchange.LeaderResponseMessage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * @Author: zhangshuang
 * @Date: 2020/3/4 11:33 AM
 * Version 1.0
 */
public class HeartBeatTest {

    private static int nodeNums = 4;

    private static final ExecutorService nodeStartPools = Executors.newCachedThreadPool();

    private static AtomicBoolean isTestTurnOn = new AtomicBoolean(false);

    private static AtomicBoolean isTestTurnOn1 = new AtomicBoolean(false);

    private ServiceReplica[] serviceReplicas;

    private TestNodeServer[] serverNodes;

    private HeartBeatTimer[] mockHbTimers;

    private ServerCommunicationSystem[] serverCommunicationSystems;

    @Before
    public void serversStart() {
        CountDownLatch servers = new CountDownLatch(nodeNums);

        serviceReplicas = new ServiceReplica[4];

        serverNodes = new TestNodeServer[4];

        mockHbTimers = new HeartBeatTimer[4];

        serverCommunicationSystems = new ServerCommunicationSystem[4];

        //start 4 node servers
        for (int i = 0; i < nodeNums ; i++) {
            serverNodes[i] = new TestNodeServer(i);
            TestNodeServer node = serverNodes[i];
            nodeStartPools.execute(() -> {
                node.startNode();
                servers.countDown();
            });
        }

        try {
            servers.await();
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (int i = 0; i < nodeNums; i++) {
            serviceReplicas[i] = serverNodes[i].getReplica();
            mockHbTimers[i] = serviceReplicas[i].getHeartBeatTimer();
            serverCommunicationSystems[i] = serviceReplicas[i].getServerCommunicationSystem();
        }
    }


    @Test
    public void stopLeaderHbTest() {

        //stop leader's heart beat, not network reason
        stopLeaderHeartBeat(serviceReplicas);

        try {
            Thread.sleep(Integer.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void oneUnleaderHbTimeoutTest() {
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(30000);
                return invocationOnMock.callRealMethod();
            }
        }).when(mockHbTimers[3]).receiveHeartBeatMessage(any());


        try {
            Thread.sleep(Integer.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void twoUnleaderHbTimeoutTest() {
        isTestTurnOn.set(true);

        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                if (isTestTurnOn.get()) {
                    Thread.sleep(30000);
                }
                isTestTurnOn.set(false);
                return invocationOnMock.callRealMethod();
            }
        }).when(mockHbTimers[3]).receiveHeartBeatMessage(any());

        isTestTurnOn1.set(true);
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                if (isTestTurnOn1.get()) {
                    Thread.sleep(30000);
                }
                isTestTurnOn1.set(false);
                return invocationOnMock.callRealMethod();
            }
        }).when(mockHbTimers[2]).receiveHeartBeatMessage(any());

        try {
            Thread.sleep(Integer.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Leader节点发送心跳超时，触发其他节点领导者改变，然后此时Leader节点启动
     */
    @Test
    public void leaderHbTimeoutAndRestartTest() {
        // 重新设置leader的消息处理方式
        MessageHandler mockMessageHandler = spy(serverCommunicationSystems[0].getMessageHandler());

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
                        System.out.println("receive leader change message !");
                    } else if (obj instanceof LeaderResponseMessage) {
                        System.out.println("receive leader response message !");
                        invocationOnMock.callRealMethod();
                    } else if (obj instanceof HeartBeatMessage) {
                        System.out.printf("0 receive heart beat message from %s !\r\n", ((HeartBeatMessage) obj).getSender());
                        invocationOnMock.callRealMethod();
                    } else {
                        invocationOnMock.callRealMethod();
                    }
                }
                return null;
            }
        }).when(mockMessageHandler).processData(any());

        serverCommunicationSystems[0].setMessageHandler(mockMessageHandler);

        // 领导者心跳停止
        stopLeaderHeartBeat(serviceReplicas);

        System.out.println("-- stopLeaderHeartBeat");

        try {
            // 休眠20s
            Thread.sleep(60000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        restartLeaderHeartBeat(serviceReplicas);

        System.out.println("-- restartLeaderHeartBeat");

        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(30000);
                return invocationOnMock.callRealMethod();
            }
        }).when(mockHbTimers[0]).receiveHeartBeatMessage(any());


        try {
            Thread.sleep(Integer.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    @Test
    public void clientSendTest() {
        int clientProcId = 11000;

        // create client request content
//        Random random = new Random();
//        byte[] bytes = new byte[4];
//        random.nextBytes(bytes);

        //create client proxy
//        AsynchServiceProxy clientProxy = new AsynchServiceProxy(clientProcId);

        //simple send msg test
//        clientProxy.invokeOrdered(bytes);

    }

    private void stopLeaderHeartBeat(ServiceReplica[] serviceReplicas) {

        int leadId = serviceReplicas[0].getTomLayer().getExecManager().getCurrentLeader();

        serviceReplicas[leadId].getTomLayer().heartBeatTimer.stopAll();
    }

    private void restartLeaderHeartBeat(ServiceReplica[] serviceReplicas) {

        int leadId = serviceReplicas[0].getTomLayer().getExecManager().getCurrentLeader();

        serviceReplicas[leadId].getTomLayer().heartBeatTimer.restart();
    }
}
