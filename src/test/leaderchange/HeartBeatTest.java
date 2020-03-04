package test.leaderchange;

import bftsmart.tom.AsynchServiceProxy;
import bftsmart.tom.ServiceReplica;

import java.util.Random;

/**
 * @Author: zhangshuang
 * @Date: 2020/3/4 11:33 AM
 * Version 1.0
 */
public class HeartBeatTest {

    private static int nodeNums = 4;


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

        ServiceReplica[] replicas;

        //start 4 node servers
        for (int i = 0; i < nodeNums ; i++) {
           new NodeServerTest(i);
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
