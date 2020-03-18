package test.bftsmart.leaderchange;

import bftsmart.communication.MessageHandler;
import bftsmart.communication.ServerCommunicationSystem;
import bftsmart.tom.AsynchServiceProxy;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.leaderchange.HeartBeatMessage;
import bftsmart.tom.leaderchange.HeartBeatTimer;
import bftsmart.tom.leaderchange.LCMessage;
import bftsmart.tom.leaderchange.LeaderResponseMessage;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * @Author: zhangshuang
 * @Date: 2020/3/18 1:57 PM
 * Version 1.0
 */
public class ConsensusTest {

    private static final ExecutorService nodeStartPools = Executors.newCachedThreadPool();

    private ServiceReplica[] serviceReplicas;

    private TestNodeServer[] serverNodes;

    private HeartBeatTimer[] mockHbTimers;

    private ServerCommunicationSystem[] serverCommunicationSystems;

    private  static int clientProcId = 11000;

    private AsynchServiceProxy clientProxy;

    private byte[] bytes;

    @Before
    public void createClient() {
        clientProxy = new AsynchServiceProxy(clientProcId);
        Random random = new Random();
        bytes = new byte[4];
        random.nextBytes(bytes);

    }

    /**
     * simple test when consensus and heart beat all run in 4 nodes
     */
    @Test
    public void test4NodeNormalConsensus() {

        int nodeNum = 4;

        initNode(nodeNum);

        //simple send msg test
        clientProxy.invokeOrdered(bytes);

        try {
            System.out.println("-- client send finish --");
            Thread.sleep(Integer.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 开始一段时间正常共识，然后领导者异常循环切换
     */
    @Test
    public void test4NodeFirstNormalConsensusThenLeaderRollException() {


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

        // 首先删除view，然后修改配置文件
        try {
            String path = HeartBeatForOtherSizeTest.class.getResource("/").toURI().getPath();
            String dirPath = new File(path).getParentFile().getParentFile().getPath() + File.separator + "config";
            // 删除view
            new File(dirPath + File.separator + "currentView").delete();
            // 删除system文件
            new File(dirPath + File.separator + "system.config").delete();

            // 根据nodeSize，重新copy一份system.config文件
            File needSystemConfig = new File(dirPath + File.separator + "system_" + nodeSize + ".config");

            // copy一份system.config
            FileUtils.copyFile(needSystemConfig, new File(dirPath + File.separator + "system.config"));
        } catch (Exception e) {
            e.printStackTrace();
        }

        CountDownLatch servers = new CountDownLatch(nodeSize);

        serviceReplicas = new ServiceReplica[nodeSize];

        serverNodes = new TestNodeServer[nodeSize];

        mockHbTimers = new HeartBeatTimer[nodeSize];

        serverCommunicationSystems = new ServerCommunicationSystem[nodeSize];

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
            mockHbTimers[i] = serviceReplicas[i].getHeartBeatTimer();
            serverCommunicationSystems[i] = serviceReplicas[i].getServerCommunicationSystem();
        }
    }

    private void stopLeaderHeartBeat(ServiceReplica[] serviceReplicas) {

        int leadId = serviceReplicas[0].getTomLayer().getExecManager().getCurrentLeader();

        serviceReplicas[leadId].getTomLayer().heartBeatTimer.stopAll();
    }

}
