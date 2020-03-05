package test.leaderchange;

import bftsmart.communication.ServerCommunicationSystem;
import bftsmart.communication.SystemMessage;
import bftsmart.tom.AsynchServiceProxy;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.leaderchange.HeartBeatMessage;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;

/**
 * @Author: zhangshuang
 * @Date: 2020/3/4 11:33 AM
 * Version 1.0
 */
public class HeartBeatTest {

    private static int nodeNums = 4;

    private static final ExecutorService nodeStartPools = Executors.newCachedThreadPool();

    private static AtomicBoolean isTestTurnOn = new AtomicBoolean(false);

    public static void main(String[] args) {

        CountDownLatch servers = new CountDownLatch(nodeNums);

        ServiceReplica[] serviceReplicas = new ServiceReplica[4];

        int clientProcId = 11000;

        // create client request content
        Random random = new Random();
        byte[] bytes = new byte[4];
        random.nextBytes(bytes);

        NodeServerTest[] serverNodes = new NodeServerTest[4];

        //start 4 node servers
        for (int i = 0; i < nodeNums ; i++) {
           serverNodes[i] = new NodeServerTest(i);
           NodeServerTest node = serverNodes[i];
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

        for (int i = 0; i < 4; i++) {
            serviceReplicas[i] = serverNodes[i].getReplica();
        }

        //create client proxy
        AsynchServiceProxy clientProxy = new AsynchServiceProxy(clientProcId);

        //simple send msg test
        clientProxy.invokeOrdered(bytes);

        //mock cs
        mockCommunicationSystem(serviceReplicas);

        // test1
        leaderHeartbeatTimeoutTest(serviceReplicas[0]);


        // test2
        // TODO: 2020/3/4  


    }

    public static void mockCommunicationSystem(ServiceReplica[] serviceReplicas)  {

        ServerCommunicationSystem realCommunicationSystem0 = serviceReplicas[0].getServerCommunicationSystem();
        ServerCommunicationSystem realCommunicationSystem1 = serviceReplicas[1].getServerCommunicationSystem();
        ServerCommunicationSystem realCommunicationSystem2 = serviceReplicas[2].getServerCommunicationSystem();
        ServerCommunicationSystem realCommunicationSystem3 = serviceReplicas[3].getServerCommunicationSystem();

        ServerCommunicationSystem mockCommunicationSystem0 = Mockito.spy(realCommunicationSystem0);
        ServerCommunicationSystem mockCommunicationSystem1 = Mockito.spy(realCommunicationSystem1);
        ServerCommunicationSystem mockCommunicationSystem2 = Mockito.spy(realCommunicationSystem2);
        ServerCommunicationSystem mockCommunicationSystem3 = Mockito.spy(realCommunicationSystem3);

        serviceReplicas[0].setCommunicationSystem(mockCommunicationSystem0);
        serviceReplicas[1].setCommunicationSystem(mockCommunicationSystem1);
        serviceReplicas[2].setCommunicationSystem(mockCommunicationSystem2);
        serviceReplicas[3].setCommunicationSystem(mockCommunicationSystem3);

    }
    public static void leaderHeartbeatTimeoutTest(ServiceReplica serviceReplica) {


        ServerCommunicationSystem mockCommunicationSystem = serviceReplica.getServerCommunicationSystem();

        isTestTurnOn.set(true);

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                if (isTestTurnOn.get()) {
                    if (invocation.getArguments()[1] instanceof HeartBeatMessage) {
                        Thread.sleep(20000);
                    }
                }
                return invocation.callRealMethod();
            }
        }).when(mockCommunicationSystem).send(any(), any());

        isTestTurnOn.set(false);
    }









}
