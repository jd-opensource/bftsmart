package test.leaderchange;

import bftsmart.tom.AsynchServiceProxy;
import bftsmart.tom.ServiceReplica;

import java.util.Random;
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

//        if (Integer.parseInt(args[0]) < 4) {
//            System.out.println("Client proc id error, cann't same with node server!!");
//            return;
//        }

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
            });
        }

        //create client proxy
        AsynchServiceProxy clientProxy = new AsynchServiceProxy(clientProcId);

        clientProxy.invokeOrdered(bytes);

        // test1
        // TODO: 2020/3/4  
        
        // test2
        // TODO: 2020/3/4  


    }






}
