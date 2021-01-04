package test.bftsmart.leaderchange;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import bftsmart.communication.MessageHandler;
import bftsmart.communication.ServerCommunicationSystem;
import bftsmart.communication.ServerCommunicationSystemImpl;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.leaderchange.HeartBeatMessage;
import bftsmart.tom.leaderchange.LCMessage;
import bftsmart.tom.leaderchange.LeaderResponseMessage;

/**
 * @Author: zhangshuang
 * @Date: 2020/3/4 11:33 AM
 * Version 1.0
 */
public class HeartBeatTest_ {

    private static int nodeNums = 4;

    private static final ExecutorService nodeStartPools = Executors.newCachedThreadPool();

    private static AtomicBoolean isTestTurnOn = new AtomicBoolean(false);

    private static AtomicBoolean isTestTurnOn1 = new AtomicBoolean(false);

    private ServiceReplica[] serviceReplicas;

    private TestNodeServer[] serverNodes;

//    private HeartBeatTimer[] mockHbTimers;

    private ServerCommunicationSystemImpl[] serverCommunicationSystems;

    @Before
    public void serversStart() {
        initNode(nodeNums);
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

//    @Test
//    public void oneUnleaderHbTimeoutTest() {
//        Mockito.doAnswer(new Answer() {
//            @Override
//            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
//                Thread.sleep(30000);
//                return invocationOnMock.callRealMethod();
//            }
//        }).when(mockHbTimers[3]).receiveHeartBeatMessage(any());
//
//
//        try {
//            Thread.sleep(Integer.MAX_VALUE);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

//    @Test
//    public void twoUnleaderHbTimeoutTest() {
//        isTestTurnOn.set(true);
//
//        Mockito.doAnswer(new Answer() {
//            @Override
//            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
//                if (isTestTurnOn.get()) {
//                    Thread.sleep(30000);
//                }
//                isTestTurnOn.set(false);
//                return invocationOnMock.callRealMethod();
//            }
//        }).when(mockHbTimers[3]).receiveHeartBeatMessage(any());
//
//        isTestTurnOn1.set(true);
//        Mockito.doAnswer(new Answer() {
//            @Override
//            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
//                if (isTestTurnOn1.get()) {
//                    Thread.sleep(30000);
//                }
//                isTestTurnOn1.set(false);
//                return invocationOnMock.callRealMethod();
//            }
//        }).when(mockHbTimers[2]).receiveHeartBeatMessage(any());
//
//        try {
//            Thread.sleep(Integer.MAX_VALUE);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }


    /**
     * 领导者少发了部分消息，但是不会触发超时
     * 理论上不应该触发领导者切换
     */
    @Test
    public void leaderStopButUnTimeoutTest() {
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
            Thread.sleep(10000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 重启之前领导者心跳服务
        restartLeaderHeartBeat(serviceReplicas, 0);

        System.out.println("-- restartLeaderHeartBeat");

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

        // 重启之前领导者心跳服务
        restartLeaderHeartBeat(serviceReplicas, 0);

        System.out.println("-- restartLeaderHeartBeat");

        try {
            Thread.sleep(Integer.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * leader超时重启，然后下一个被选中的leader也如此，检查是否可正常
     */
    @Test
    public void leaderHbTimeoutAndRestartAndNextLeaderRepeatTest() {
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
                        System.out.println("0 receive leader change message !");
                    } else if (obj instanceof LeaderResponseMessage) {
                        System.out.println("0 receive leader response message !");
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

        System.out.println("-- stopLeaderHeartBeat --");

        try {
            // 休眠20s
            Thread.sleep(60000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 重启之前领导者心跳服务
        restartLeaderHeartBeat(serviceReplicas, 0);

        System.out.println("-- restartLeaderHeartBeat --");

        // 重置mock操作
        reset(mockMessageHandler);

        try {
            Thread.sleep(30000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 当前领导者异常，当前领导者为peer1
        mockMessageHandler = spy(serverCommunicationSystems[1].getMessageHandler());

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
                        System.out.println("1 receive leader change message !");
                    } else if (obj instanceof LeaderResponseMessage) {
                        System.out.println("1 receive leader response message !");
                        invocationOnMock.callRealMethod();
                    } else if (obj instanceof HeartBeatMessage) {
                        System.out.printf("1 receive heart beat message from %s !\r\n", ((HeartBeatMessage) obj).getSender());
                        invocationOnMock.callRealMethod();
                    } else {
                        invocationOnMock.callRealMethod();
                    }
                }
                return null;
            }
        }).when(mockMessageHandler).processData(any());

        serverCommunicationSystems[1].setMessageHandler(mockMessageHandler);

        // 领导者心跳停止
        stopLeaderHeartBeat(serviceReplicas);

        System.out.println("-- stopLeaderHeartBeat --");

        try {
            // 休眠20s
            Thread.sleep(60000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 重启之前领导者心跳服务
        restartLeaderHeartBeat(serviceReplicas, 1);

        System.out.println("-- restartLeaderHeartBeat --");

        // 重置mock操作
        reset(mockMessageHandler);


        try {
            Thread.sleep(Integer.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试4个节点循环进行一次领导者切换，看最终是否可以恢复正常
     */
    @Test
    public void test4NodeLeaderChange() {

        for (int i = 0; i < nodeNums; i++) {

            final int index = i;

            // 重新设置leader的消息处理方式
            MessageHandler mockMessageHandler = spy(serverCommunicationSystems[i].getMessageHandler());

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
                            System.out.println(index + " receive leader change message !");
                        } else if (obj instanceof LeaderResponseMessage) {
                            System.out.println(index + " receive leader response message !");
                            invocationOnMock.callRealMethod();
                        } else if (obj instanceof HeartBeatMessage) {
                            System.out.printf(index + " receive heart beat message from %s !\r\n", ((HeartBeatMessage) obj).getSender());
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

    private void initNode(int nodeSize) {
        CountDownLatch servers = new CountDownLatch(nodeSize);

        serviceReplicas = new ServiceReplica[nodeSize];

        serverNodes = new TestNodeServer[nodeSize];

//        mockHbTimers = new HeartBeatTimer[nodeSize];

        serverCommunicationSystems = new ServerCommunicationSystemImpl[nodeSize];

        //start nodeSize node servers
        for (int i = 0; i < nodeSize ; i++) {
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

        for (int i = 0; i < nodeSize; i++) {
            serviceReplicas[i] = serverNodes[i].getReplica();
//            mockHbTimers[i] = serviceReplicas[i].getHeartBeatTimer();
            serverCommunicationSystems[i] = (ServerCommunicationSystemImpl) serviceReplicas[i].getServerCommunicationSystem();
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
}
