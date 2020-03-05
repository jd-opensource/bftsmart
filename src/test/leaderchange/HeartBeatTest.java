package test.leaderchange;

import bftsmart.communication.ServerCommunicationSystem;
import bftsmart.tom.AsynchServiceProxy;
import bftsmart.tom.ServiceReplica;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Author: zhangshuang
 * @Date: 2020/3/4 11:33 AM
 * Version 1.0
 */
public class HeartBeatTest {

    private static int nodeNums = 4;

    private static final ExecutorService nodeStartPools = Executors.newCachedThreadPool();


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

        clientProxy.invokeOrdered(bytes);

        // test1
        leaderHeartbeatTimeoutTest(serviceReplicas);

        
        // test2
        // TODO: 2020/3/4  


    }

    public static void leaderHeartbeatTimeoutTest(ServiceReplica[] serviceReplicas) {

        ServerCommunicationSystem realCommunicationSystem0 = serviceReplicas[0].getServerCommunicationSystem();
        ServerCommunicationSystem realCommunicationSystem1 = serviceReplicas[1].getServerCommunicationSystem();
        ServerCommunicationSystem realCommunicationSystem2 = serviceReplicas[2].getServerCommunicationSystem();
        ServerCommunicationSystem realCommunicationSystem3 = serviceReplicas[3].getServerCommunicationSystem();

//        ServerCommunicationSystem mockCommunicationSystem =
    }









}
