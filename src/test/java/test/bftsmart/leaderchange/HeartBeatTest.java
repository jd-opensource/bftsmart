package test.bftsmart.leaderchange;

import bftsmart.tom.ServiceReplica;
import bftsmart.tom.leaderchange.HeartBeatTimer;
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

/**
 * @Author: zhangshuang
 * @Date: 2020/3/4 11:33 AM
 * Version 1.0
 */
public class HeartBeatTest {

    private static int nodeNums = 4;

    private static final ExecutorService nodeStartPools = Executors.newCachedThreadPool();

    private static AtomicBoolean isTestTurnOn = new AtomicBoolean(false);

    @Test
    public void stopLeaderHbTest() {

        CountDownLatch servers = new CountDownLatch(nodeNums);

        ServiceReplica[] serviceReplicas = new ServiceReplica[4];

        TestNodeServer[] serverNodes = new TestNodeServer[4];

        HeartBeatTimer[] mockHbTimers = new HeartBeatTimer[4];

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
        }

        //stop leader's heart beat, not network reason
        stopLeaderHeartBeat(serviceReplicas);

        try {
            Thread.sleep(Integer.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    @Test
    public void UnleaderHbTimeoutTest() {
        CountDownLatch servers = new CountDownLatch(nodeNums);

        ServiceReplica[] serviceReplicas = new ServiceReplica[4];

        TestNodeServer[] serverNodes = new TestNodeServer[4];

        HeartBeatTimer[] mockHbTimers = new HeartBeatTimer[4];

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
        }

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
}
