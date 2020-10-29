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
package bftsmart.communication.server;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import bftsmart.communication.SystemMessage;
import bftsmart.communication.queue.MessageQueue;
import bftsmart.communication.queue.MessageQueueFactory;
import bftsmart.communication.server.timestamp.TimestampVerifyService;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.reconfiguration.VMMessage;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.util.TOMUtil;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.HashSet;

/**
 * This class represents a connection with other server.
 *
 * ServerConnections are created by ServerCommunicationLayer.
 *
 * @author alysson
 */
public class ServerConnection {

    public static final String MAC_ALGORITHM = "HmacMD5";
    private static final long POOL_TIME = 5000;
    //private static final int SEND_QUEUE_SIZE = 50;
    private ServerViewController controller;
    private Socket socket;
    private DataOutputStream socketOutStream = null;
    private DataInputStream socketInStream = null;
    private int remoteId;
    private boolean useSenderThread;
    private MessageQueue messageInQueue;
    protected LinkedBlockingQueue<byte[]> outQueue;// = new LinkedBlockingQueue<byte[]>(SEND_QUEUE_SIZE);
    private HashSet<Integer> noMACs = null; // this is used to keep track of data to be sent without a MAC.
                                            // It uses the reference id for that same data
//    private LinkedBlockingQueue<SystemMessage> inQueue;
    private SecretKey authKey = null;
    private Mac macSend;
    private Mac macReceive;
    private int macSize;
    private Lock connectLock = new ReentrantLock();
    /** Only used when there is no sender Thread */
    private Lock sendLock;
    private boolean doWork = true;
    private CountDownLatch latch = new CountDownLatch(1);

    private CountDownLatch initSuccessLatch = new CountDownLatch(1);

    private ServiceReplica replica;

    private volatile boolean canSendMessage = false;

    private TimestampVerifyService timestampVerifyService;

    private volatile boolean currSocketTimestampOver = false;

    private final ExecutorService startConnectService = Executors.newSingleThreadExecutor();

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ServerConnection.class);

    public ServerConnection(ServerViewController controller, Socket socket,
                            int remoteId, MessageQueue messageInQueue, ServiceReplica replica,
                            TimestampVerifyService timestampVerifyService) {

        this.controller = controller;

        this.replica = replica;

        this.socket = socket;

        this.remoteId = remoteId;

        this.messageInQueue = messageInQueue;

        this.timestampVerifyService = timestampVerifyService;

        this.outQueue = new LinkedBlockingQueue<byte[]>(this.controller.getStaticConf().getOutQueueSize());

        this.noMACs = new HashSet<Integer>();
        // Connect to the remote process or just wait for the connection?
        if (isToConnect()) {
            //I have to connect to the remote server
            try {
                this.socket = new Socket(this.controller.getStaticConf().getHost(remoteId),
                        this.controller.getStaticConf().getServerToServerPort(remoteId));
                ServersCommunicationLayer.setSocketOptions(this.socket);
                new DataOutputStream(this.socket.getOutputStream()).writeInt(this.controller.getStaticConf().getProcessId());

            } catch (UnknownHostException ex) {
                ex.printStackTrace();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        //else I have to wait a connection from the remote server

        if (this.socket != null) {
            try {
                socketOutStream = new DataOutputStream(this.socket.getOutputStream());
                socketInStream = new DataInputStream(this.socket.getInputStream());
                Executors.newSingleThreadExecutor().execute(() -> {
                    connectLock.lock();
                    try {
                        if (authTimestamp()) {
                            authKey = null;
                            LOGGER.info("I am {}, set remote[{}]'s authKey = NULL !!!", this.controller.getStaticConf().getProcessId(), remoteId);
                            authenticateAndEstablishAuthKey();
                        }
                    } finally {
                        connectLock.unlock();
                    }
                });
            } catch (IOException ex) {
                LOGGER.error("Error creating connection to {}", remoteId);
                ex.printStackTrace();
            }
        }
               
       //******* EDUARDO BEGIN **************//
        this.useSenderThread = this.controller.getStaticConf().isUseSenderThread();

        if (useSenderThread && (this.controller.getStaticConf().getTTPId() != remoteId)) {
            new SenderThread(latch).start();
        } else {
            sendLock = new ReentrantLock();
        }
//        monitorReconnect(null);
        Executors.newSingleThreadExecutor().execute(() -> {
            while (doWork) {
                try {
                    Thread.sleep(5000);
                    monitorReconnect(this.socket);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        if (!this.controller.getStaticConf().isTheTTP()) {
            if (this.controller.getStaticConf().getTTPId() == remoteId) {
                //Uma thread "diferente" para as msgs recebidas da TTP
                new TTPReceiverThread(replica).start();
            } else {
                new ReceiverThread().start();
            }
        }
        //******* EDUARDO END **************//
    }


//    public ServerConnection(ServerViewController controller, Socket socket, int remoteId,
//                            LinkedBlockingQueue<SystemMessage> inQueue, ServiceReplica replica) {
//
//        this.controller = controller;
//
//        this.socket = socket;
//
//        this.remoteId = remoteId;
//
//        this.inQueue = inQueue;
//
//        this.outQueue = new LinkedBlockingQueue<byte[]>(this.controller.getStaticConf().getOutQueueSize());
//
//        this.noMACs = new HashSet<Integer>();
//        // Connect to the remote process or just wait for the connection?
//        if (isToConnect()) {
//            //I have to connect to the remote server
//            try {
//                this.socket = new Socket(this.controller.getStaticConf().getHost(remoteId),
//                        this.controller.getStaticConf().getServerToServerPort(remoteId));
//                ServersCommunicationLayer.setSocketOptions(this.socket);
//                new DataOutputStream(this.socket.getOutputStream()).writeInt(this.controller.getStaticConf().getProcessId());
//
//            } catch (UnknownHostException ex) {
//                ex.printStackTrace();
//            } catch (IOException ex) {
//                ex.printStackTrace();
//            }
//        }
//        //else I have to wait a connection from the remote server
//
//        if (this.socket != null) {
//            try {
//                socketOutStream = new DataOutputStream(this.socket.getOutputStream());
//                socketInStream = new DataInputStream(this.socket.getInputStream());
//            } catch (IOException ex) {
//                LOGGER.debug("Error creating connection to "+remoteId);
//                ex.printStackTrace();
//            }
//        }
//
//        //******* EDUARDO BEGIN **************//
//        this.useSenderThread = this.controller.getStaticConf().isUseSenderThread();
//
//        if (useSenderThread && (this.controller.getStaticConf().getTTPId() != remoteId)) {
//            new SenderThread(latch).start();
//        } else {
//            sendLock = new ReentrantLock();
//        }
//        authenticateAndEstablishAuthKey();
//
//        if (!this.controller.getStaticConf().isTheTTP()) {
//            if (this.controller.getStaticConf().getTTPId() == remoteId) {
//                //Uma thread "diferente" para as msgs recebidas da TTP
//                new TTPReceiverThread(replica).start();
//            } else {
//                new ReceiverThread().start();
//            }
//        }
//        //******* EDUARDO END **************//
//    }

    public SecretKey getSecretKey() {
        return authKey;
    }
    
    /**
     * Stop message sending and reception.
     */
    public void shutdown() {
        LOGGER.info("SHUTDOWN for {}", remoteId);
        
        doWork = false;
        closeSocket();
    }

    /**
     * Used to send packets to the remote server.
     */
    public final void send(byte[] data, boolean useMAC) throws InterruptedException {
        if (useSenderThread) {
            //only enqueue messages if there queue is not full
            if (!useMAC) {
                LOGGER.debug("(ServerConnection.send) Not sending defaultMAC {}", System.identityHashCode(data));
                noMACs.add(System.identityHashCode(data));
            }
            LOGGER.info("I am {}, send data to remote {} !", this.controller.getStaticConf().getProcessId(), remoteId);
            if (!outQueue.offer(data)) {
                LOGGER.error("(ServerConnection.send) out queue for {} full (message discarded).", remoteId);
            }
        } else {
            sendLock.lock();
            sendBytes(data, useMAC);
            sendLock.unlock();
        }
    }

    /**
     * try to send a message through the socket
     * if some problem is detected, a reconnection is done
     */
    private final void sendBytes(byte[] messageData, boolean useMAC) {       
        boolean abort = false;
        do {
            if (abort) return; // if there is a need to reconnect, abort this method
            if (socket != null && socketOutStream != null) {
                try {
                    //do an extra copy of the data to be sent, but on a single out stream write
                    byte[] mac = (useMAC && this.controller.getStaticConf().getUseMACs() == 1)?macSend.doFinal(messageData):null;
                    byte[] data = new byte[5 +messageData.length+((mac!=null)?mac.length:0)];
                    int value = messageData.length;

                    System.arraycopy(new byte[]{(byte)(value >>> 24),(byte)(value >>> 16),(byte)(value >>> 8),(byte)value},0,data,0,4);
                    System.arraycopy(messageData,0,data,4,messageData.length);
                    if(mac != null) {
                        //System.arraycopy(mac,0,data,4+messageData.length,mac.length);
                        System.arraycopy(new byte[]{ (byte) 1},0,data,4+messageData.length,1);
                        System.arraycopy(mac,0,data,5+messageData.length,mac.length);
                    } else {
                        System.arraycopy(new byte[]{(byte) 0},0,data,4+messageData.length,1);                        
                    }

                    socketOutStream.write(data);

                    return;
                } catch (IOException ex) {
                    LOGGER.error("[ServerConnection.sendBytes] I am proc {}, I will close socket and waitAndConnect connect with {}", this.controller.getStaticConf().getProcessId(), remoteId);
                    closeSocket();
                    waitAndConnect();
                    abort = true;
                }
            } else {
                LOGGER.error("[ServerConnection.sendBytes] I am proc {}, I will waitAndConnect connect with {}", this.controller.getStaticConf().getProcessId(), remoteId);
                waitAndConnect();
                abort = true;
            }
        } while (doWork);
    }

    //******* EDUARDO BEGIN **************//
    //return true of a process shall connect to the remote process, false otherwise
    private boolean isToConnect() {
        if (this.controller.getStaticConf().getTTPId() == remoteId) {
            //Need to wait for the connection request from the TTP, do not tray to connect to it
            return false;
        } else if (this.controller.getStaticConf().getTTPId() == this.controller.getStaticConf().getProcessId()) {
            //If this is a TTP, one must connect to the remote process
            return true;
        }
        boolean ret = false;
        if (this.controller.isInCurrentView()) {
            
             //in this case, the node with higher ID starts the connection
             if (this.controller.getStaticConf().getProcessId() > remoteId) {
                 ret = true;
             }
                
            /** JCS: I commented the code below to fix a bug, but I am not sure
             whether its completely useless or not. The 'if' above was taken
             from that same code (its the only part I understand why is necessary)
             I keep the code commented just to be on the safe side*/
            
            /**
            
            boolean me = this.controller.isInLastJoinSet(this.controller.getStaticConf().getProcessId());
            boolean remote = this.controller.isInLastJoinSet(remoteId);

            //either both endpoints are old in the system (entered the system in a previous view),
            //or both entered during the last reconfiguration
            if ((me && remote) || (!me && !remote)) {
                //in this case, the node with higher ID starts the connection
                if (this.controller.getStaticConf().getProcessId() > remoteId) {
                    ret = true;
                }
            //this process is the older one, and the other one entered in the last reconfiguration
            } else if (!me && remote) {
                ret = true;

            } //else if (me && !remote) { //this process entered in the last reconfig and the other one is old
                //ret=false; //not necessary, as ret already is false
            //}
              
            */
        }
        return ret;
    }
    //******* EDUARDO END **************//

    protected void monitorReconnect(final Socket newSocket) {
//        if (!currSocketTimestampOver) {
            startConnectService.execute(() -> {
                LOGGER.info("[{}] -> I am {}, start handle remote[{}] !", this.replica.getRealName(), this.controller.getStaticConf().getProcessId(), remoteId);
                connectLock.lock();
                try {
                    if (socket == null || !socket.isConnected()) {
                        LOGGER.info("[{}] ->  I am {}, socket is null for remote[{}] !", this.replica.getRealName(), this.controller.getStaticConf().getProcessId(), remoteId);
                        try {

                            //******* EDUARDO BEGIN **************//
                            if (isToConnect()) {
                                LOGGER.info("I am {}, to connect remote[{}] !", this.controller.getStaticConf().getProcessId(), remoteId);
                                socket = new Socket(this.controller.getStaticConf().getHost(remoteId),
                                        this.controller.getStaticConf().getServerToServerPort(remoteId));
                                ServersCommunicationLayer.setSocketOptions(socket);
                                new DataOutputStream(socket.getOutputStream()).writeInt(this.controller.getStaticConf().getProcessId());

                                //******* EDUARDO END **************//
                            } else {
                                socket = newSocket;
                            }
                        } catch (UnknownHostException ex) {
                            ex.printStackTrace();
                        } catch (IOException ex) {
                            LOGGER.error("Impossible to reconnect to replica {}", remoteId);
                        }
                        if (socket != null) {
                            try {
                                socketOutStream = new DataOutputStream(socket.getOutputStream());
                                socketInStream = new DataInputStream(socket.getInputStream());
                                if (authTimestamp()) {
                                    authKey = null;
                                    LOGGER.info("[{}] -> I am {}, set remote[{}]'s authKey = NULL !!!", this.replica.getRealName(), this.controller.getStaticConf().getProcessId(), remoteId);
                                    authenticateAndEstablishAuthKey();
                                }
                            } catch (IOException ex) {
                                ex.printStackTrace();
                            }
                        }
                        LOGGER.info("[{}] ->  I am {}, remote[{}] socket = {}, newSocket = {} !!!", this.replica.getRealName(), this.controller.getStaticConf().getProcessId(), remoteId, socket == null, newSocket == null);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    connectLock.unlock();
                }
//                if (currSocketTimestampOver) {
//                    connectLock.unlock();
//                    return;
//                }

//                if (socket != null) {
//                    System.out.println("-----" + 3 + "-----");
//                    if (authTimestamp()) {
//                        authKey = null;
//                        LOGGER.info("I am {}, set remote[{}]'s authKey = NULL !!!", this.controller.getStaticConf().getProcessId(), remoteId);
//                        authenticateAndEstablishAuthKey();
//                    }
//                }
//                connectLock.unlock();
            });
//        }
    }


    /**
     * (Re-)establish connection between peers.
     *
     * @param newSocket socket created when this server accepted the connection
     * (only used if processId is less than remoteId)
     */
    protected void reconnect(final Socket newSocket) {
        connectLock.lock();
        LOGGER.info("[{}] -> I am {}, start reconnect {} !", this.replica.getRealName(), this.controller.getStaticConf().getProcessId(), remoteId);
        if (socket == null || !socket.isConnected()) {
            LOGGER.info("[{}] -> I am {}, socket is NULL, remote = {} !", this.replica.getRealName(), this.controller.getStaticConf().getProcessId(), remoteId);

            try {

                //******* EDUARDO BEGIN **************//
                if (isToConnect()) {
                    LOGGER.info("[{}] -> I am {}, socket is NULL, need to connect remote = {} !", this.replica.getRealName(), this.controller.getStaticConf().getProcessId(), remoteId);
                    socket = new Socket(this.controller.getStaticConf().getHost(remoteId),
                            this.controller.getStaticConf().getServerToServerPort(remoteId));
                    ServersCommunicationLayer.setSocketOptions(socket);
                    new DataOutputStream(socket.getOutputStream()).writeInt(this.controller.getStaticConf().getProcessId());

                    //******* EDUARDO END **************//
                } else {
                    socket = newSocket;
                }
            } catch (UnknownHostException ex) {
                ex.printStackTrace();
            } catch (IOException ex) {
                LOGGER.error("Impossible to reconnect to replica {}", remoteId);
            }
            if (socket != null) {
                try {
                    socketOutStream = new DataOutputStream(socket.getOutputStream());
                    socketInStream = new DataInputStream(socket.getInputStream());
                    if (authTimestamp()) {
                        authKey = null;
                        LOGGER.info("[{}] -> I am {}, set remote[{}]'s authKey = NULL !!!", this.replica.getRealName(), this.controller.getStaticConf().getProcessId(), remoteId);
                        authenticateAndEstablishAuthKey();
                    }
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
            LOGGER.info("[{}] -> I am {}, remote[{}] socket = {}, newSocket = {} !!!", this.replica.getRealName(), this.controller.getStaticConf().getProcessId(), remoteId, socket == null, newSocket == null);
        }
//        if (socket != null) {
//            try {
////                socketOutStream = new DataOutputStream(socket.getOutputStream());
////                socketInStream = new DataInputStream(socket.getInputStream());
//                if (authTimestamp()) {
//                    authKey = null;
//                    LOGGER.info("I am {}, set remote[{}]'s authKey = NULL !!!", this.controller.getStaticConf().getProcessId(), remoteId);
//                    authenticateAndEstablishAuthKey();
//                }
//            } catch (Exception ex) {
//                ex.printStackTrace();
//            }
//        }

        connectLock.unlock();
    }

    /**
     * 进行时间戳验证
     *
     */
    private boolean authTimestamp() {
        connectLock.lock();
        try {
            if (socketOutStream == null || socketInStream == null) {
                return false;
            }
            boolean completed = timestampVerifyService.timeVerifyCompleted();
            try {
                socketOutStream.writeBoolean(completed);
                boolean remoteCompleted = socketInStream.readBoolean();
                LOGGER.info("[{}] -> I am {} -> {}, receive remote[{}] status = {} !", this.replica.getRealName(),
                        controller.getStaticConf().getProcessId(), completed, remoteId, remoteCompleted);
                if (remoteCompleted) {
                    // 判断本地节点是否已启动
                    if (completed) {
                        // 需要判断结果是成功还是失败
                        if (timestampVerifyService.timeVerifySuccess()) {
                            currSocketTimestampOver = true;
                            return true;
                        }
                        currSocketTimestampOver = true;
                        return false;
                    } else {
                        // 本地节点没有启动，而远端启动了，则设置等待完成
                        timestampVerifyService.waitComplete(remoteId);
                        // 等待全部完成
                        long currentTimestamp = timestampVerifyService.verifyTimestamp();
                        LOGGER.info("[{}] -> I am {}, will write time[{}] to remote[{}] !", this.replica.getRealName(),
                                controller.getStaticConf().getProcessId(), currentTimestamp, remoteId);
                        // 写入时间并等待远端的时间
                        socketOutStream.writeLong(currentTimestamp);
                        long remoteTimestamp = socketInStream.readLong();
                        LOGGER.info("[{}] -> I am {}, receive remote timestamp = {} from {} !", this.replica.getRealName(),
                                controller.getStaticConf().getProcessId(), remoteTimestamp, remoteId);
                        boolean verify = timestampVerifyService.verifyTime(currentTimestamp, remoteId, remoteTimestamp);
                        if (verify) {
                            timestampVerifyService.verifySuccess(remoteId);
                            currSocketTimestampOver = true;
                            return true;
                        } else {
                            timestampVerifyService.verifyFail(remoteId);
                            currSocketTimestampOver = true;
                            return false;
                        }
                    }
                } else {
                    // 远端未完成，但本地完成了，则发送本地的时间戳，等待远端时间戳
                    if ((completed && timestampVerifyService.timeVerifySuccess()) || currSocketTimestampOver) {
                        long currentTimestamp = System.currentTimeMillis();
                        LOGGER.info("[{}] -> I am {}, will write time[{}] to remote[{}] but not need check !",
                                this.replica.getRealName(), controller.getStaticConf().getProcessId(), currentTimestamp, remoteId);
                        socketOutStream.writeLong(currentTimestamp);
                        long remoteTimestamp = socketInStream.readLong();
                        LOGGER.info("[{}] -> I am {}, receive remote timestamp = {} from {} but not need check !",
                                this.replica.getRealName(), controller.getStaticConf().getProcessId(), remoteTimestamp, remoteId);
                        currSocketTimestampOver = true;
                        return true;
                    } else {
                        // 两者都没有完成
                        // 两者都未完成
                        timestampVerifyService.waitComplete(remoteId);
                        long currentTimestamp = timestampVerifyService.verifyTimestamp();
                        LOGGER.info("[{}] -> I am {}, will write time[{}] to remote[{}] !",
                                this.replica.getRealName(), controller.getStaticConf().getProcessId(), currentTimestamp, remoteId);
                        // 写入时间并等待远端的时间
                        socketOutStream.writeLong(currentTimestamp);
                        long remoteTimestamp = socketInStream.readLong();
                        LOGGER.info("[{}] -> I am {}, receive remote timestamp = {} from {} !",
                                this.replica.getRealName(), controller.getStaticConf().getProcessId(), remoteTimestamp, remoteId);
                        boolean verify = timestampVerifyService.verifyTime(currentTimestamp, remoteId, remoteTimestamp);
                        if (verify) {
                            timestampVerifyService.verifySuccess(remoteId);
                            currSocketTimestampOver = true;
                            return true;
                        } else {
                            timestampVerifyService.verifyFail(remoteId);
                            currSocketTimestampOver = true;
                        }
                    }
//
//
//                // 对端没有完成
//                // 判断当前节点是否完成
//                if (completed || currSocketTimestampOver) {
//                    // 当前节点已完成，则不需要再处理
//                    currSocketTimestampOver = true;
//                    return true;
//                } else {
//                    // 两者都未完成
//                    timestampVerifyService.waitComplete(remoteId);
//                    long currentTimestamp = timestampVerifyService.verifyTimestamp();
//                    LOGGER.info("I am {}, will write time[{}] to remote[{}] !",
//                            controller.getStaticConf().getProcessId(), currentTimestamp, remoteId);
//                    // 写入时间并等待远端的时间
//                    socketOutStream.writeLong(currentTimestamp);
//                    long remoteTimestamp = socketInStream.readLong();
//                    LOGGER.info("I am {}, receive remote timestamp = {} from {} !",
//                            controller.getStaticConf().getProcessId(), remoteTimestamp, remoteId);
//                    boolean verify = timestampVerifyService.verifyTime(currentTimestamp, remoteId, remoteTimestamp);
//                    if (verify) {
//                        timestampVerifyService.verifySuccess(remoteId);
//                        currSocketTimestampOver = true;
//                        return true;
//                    } else {
//                        timestampVerifyService.verifyFail(remoteId);
//                        currSocketTimestampOver = true;
//                    }
//                }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return false;
        } finally {
            connectLock.unlock();
        }
    }

    //TODO!
    public void authenticateAndEstablishAuthKey() {
        if (authKey != null || socketOutStream == null || socketInStream == null) {
            LOGGER.info("[{}] ->  I am proc {}, -- will exit with proc id {}, with port {}", this.replica.getRealName(), this.controller.getStaticConf().getProcessId(), remoteId, controller.getStaticConf().getServerToServerPort(remoteId));
            return;
        }
        try {
            //if (conf.getProcessId() > remoteId) {
            // I asked for the connection, so I'm first on the auth protocol
            //DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            //} else {
            // I received a connection request, so I'm second on the auth protocol
            //DataInputStream dis = new DataInputStream(socket.getInputStream());
            //}
            
            //Derive DH private key from replica's own RSA private key
            
            PrivateKey RSAprivKey = controller.getStaticConf().getRSAPrivateKey();
            BigInteger DHPrivKey =
                    new BigInteger(RSAprivKey.getEncoded());
            
            //Create DH public key
            BigInteger myDHPubKey =
                    controller.getStaticConf().getDHG().modPow(DHPrivKey, controller.getStaticConf().getDHP());
            
            //turn it into a byte array
            byte[] bytes = myDHPubKey.toByteArray();
            
            byte[] signature = TOMUtil.signMessage(RSAprivKey, bytes);

            if (authKey == null && socketOutStream != null && socketInStream != null) {
                //send my DH public key and signature
                socketOutStream.writeInt(bytes.length);
                socketOutStream.write(bytes);

                socketOutStream.writeInt(signature.length);
                socketOutStream.write(signature);

                LOGGER.info("[{}] ->  I am proc {}, -- have write timestamp to id {}, with port {}", this.replica.getRealName(), this.controller.getStaticConf().getProcessId(), remoteId, controller.getStaticConf().getServerToServerPort(remoteId));

                //receive remote DH public key and signature
                int dataLength = socketInStream.readInt();
                bytes = new byte[dataLength];
                int read = 0;
                do {
                    read += socketInStream.read(bytes, read, dataLength - read);

                } while (read < dataLength);

                LOGGER.info("[{}] ->  I am proc {}, -- receive data with id {}, with port {}", this.replica.getRealName(), this.controller.getStaticConf().getProcessId(), remoteId, controller.getStaticConf().getServerToServerPort(remoteId));

                byte[] remote_Bytes = bytes;

                dataLength = socketInStream.readInt();
                bytes = new byte[dataLength];
                read = 0;
                do {
                    read += socketInStream.read(bytes, read, dataLength - read);

                } while (read < dataLength);

                LOGGER.info("[{}] ->  I am proc {}, -- receive signature with id {}, with port {}", this.replica.getRealName(), this.controller.getStaticConf().getProcessId(), remoteId, controller.getStaticConf().getServerToServerPort(remoteId));

                byte[] remote_Signature = bytes;

                //verify signature
                PublicKey remoteRSAPubkey = controller.getStaticConf().getRSAPublicKey(remoteId);

                if (!TOMUtil.verifySignature(remoteRSAPubkey, remote_Bytes, remote_Signature)) {

                    LOGGER.error("{} sent an invalid signature!", remoteId);
                    shutdown();
                    return;
                }

                BigInteger remoteDHPubKey = new BigInteger(remote_Bytes);

                //Create secret key
                BigInteger secretKey =
                        remoteDHPubKey.modPow(DHPrivKey, controller.getStaticConf().getDHP());

                LOGGER.info("[{}] ->  I am proc {}, -- Diffie-Hellman complete with proc id {}, with port {}", this.replica.getRealName(), this.controller.getStaticConf().getProcessId(), remoteId, controller.getStaticConf().getServerToServerPort(remoteId));

                SecretKeyFactory fac = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
                PBEKeySpec spec = new PBEKeySpec(secretKey.toString().toCharArray());

                //PBEKeySpec spec = new PBEKeySpec(PASSWORD.toCharArray());
                authKey = fac.generateSecret(spec);

                macSend = Mac.getInstance(MAC_ALGORITHM);
                macSend.init(authKey);
                macReceive = Mac.getInstance(MAC_ALGORITHM);
                macReceive.init(authKey);
                macSize = macSend.getMacLength();
                latch.countDown();
				canSendMessage = true;
                initSuccessLatch.countDown();
            } else {
                LOGGER.info("[{}] ->  I am proc {}, authKey = {} && socketOutStream = {} && socketInStream = {}",
                        this.replica.getRealName(), this.controller.getStaticConf().getProcessId(), authKey == null, socketOutStream == null, socketInStream == null);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void closeSocket() {
        if (socket != null) {
            try {
                socketOutStream.flush();
                socket.close();
            } catch (IOException ex) {
                LOGGER.error("Error closing socket to {}", remoteId);
            } catch (NullPointerException npe) {
            	LOGGER.error("Socket already closed");
            }

            socket = null;
            socketOutStream = null;
            socketInStream = null;
            currSocketTimestampOver = false;
        }
    }

    private void waitAndConnect() {
        if (doWork) {
            try {
                Thread.sleep(POOL_TIME);
            } catch (InterruptedException ie) {
            }

            outQueue.clear();
            reconnect(null);
        }
    }

    /**
     * Thread used to send packets to the remote server.
     */
    private class SenderThread extends Thread {

        private CountDownLatch countDownLatch;

        public SenderThread(CountDownLatch countDownLatch) {
            super("Sender for " + remoteId);
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            byte[] data = null;
            try {
                countDownLatch.await();
                // 等待时间戳OK
                initSuccessLatch.await();
            } catch (Exception e) {
                e.printStackTrace();
            }
            LOGGER.info("Start handle send service to {} !", remoteId);
            while (doWork) {
                //get a message to be sent
                try {
                    data = outQueue.poll(POOL_TIME, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ex) {
                }
                boolean canSend = true;
//                // 此处加锁控制
//                connectLock.lock();
//                try {
//                    // 判断连接是否正常
//                    if (authKey != null && socket != null && socketInStream != null && socketOutStream != null) {
//                        canSend = true;
//                    }
//                } finally {
//                    connectLock.unlock();
//                }

                if (data != null && canSend) {
                    LOGGER.info("[{}] -> I am {}, send data to {} !!!!!", replica.getRealName(), controller.getStaticConf().getProcessId(), remoteId);
                    //sendBytes(data, noMACs.contains(System.identityHashCode(data)));
                    int ref = System.identityHashCode(data);
                    boolean sendMAC = !noMACs.remove(ref);
                    LOGGER.debug("(ServerConnection.run) {} MAC for data {}", (sendMAC ? "Sending" : "Not sending"), ref);
                    LOGGER.info("[{}] -> I am {}, send data to {} !!!", replica.getRealName(), controller.getStaticConf().getProcessId(), remoteId);
                    sendBytes(data, sendMAC);
                }
            }

            LOGGER.debug("Sender for {} stopped!", remoteId);
        }
    }

    /**
     * Thread used to receive packets from the remote server.
     */
    protected class ReceiverThread extends Thread {

        public ReceiverThread() {
            super("Receiver for " + remoteId);
        }

        @Override
        public void run() {
            byte[] receivedMac = null;
            try {
                receivedMac = new byte[Mac.getInstance(MAC_ALGORITHM).getMacLength()];
                // 等待时间戳OK
                initSuccessLatch.await();
            } catch (Exception ex) {
                ex.printStackTrace();
            }

            LOGGER.info("Start handle receive service to {} !", remoteId);
            while (doWork) {
                if (socket != null && socketInStream != null) {
                    try {
                        //read data length
                        int dataLength = socketInStream.readInt();
                        byte[] data = new byte[dataLength];

                        //read data
                        int read = 0;
                        do {
                            read += socketInStream.read(data, read, dataLength - read);
                        } while (read < dataLength);

                        //read mac
                        boolean result = true;
                        
                        byte hasMAC = socketInStream.readByte();
                        if (controller.getStaticConf().getUseMACs() == 1 && hasMAC == 1) {
                            read = 0;
                            do {
                                read += socketInStream.read(receivedMac, read, macSize - read);
                            } while (read < macSize);

                            result = Arrays.equals(macReceive.doFinal(data), receivedMac);
                        }

                        if (result) {
                            SystemMessage sm = (SystemMessage) (new ObjectInputStream(new ByteArrayInputStream(data)).readObject());
                            sm.authenticated = (controller.getStaticConf().getUseMACs() == 1 && hasMAC == 1);
                            
                            if (sm.getSender() == remoteId) {

                                MessageQueue.MSG_TYPE msgType = MessageQueueFactory.msgType(sm);

                                if (!messageInQueue.offer(msgType, sm)) {
                                    LOGGER.error("(ReceiverThread.run) in queue full (message from {} discarded).", remoteId);
                                }
                            }
                        } else {
                            //TODO: violation of authentication... we should do something
                            LOGGER.warn("WARNING: Violation of authentication in message received from {}", remoteId);
                        }
                    } catch (ClassNotFoundException ex) {
                        //invalid message sent, just ignore;
                    } catch (IOException ex) {
                        if (doWork) {
                            LOGGER.warn("[ServerConnection.ReceiverThread] I will close socket and waitAndConnect connect with {}", remoteId);
                            closeSocket();
                            waitAndConnect();
                        }
                    }
                } else {
                    LOGGER.error("[ServerConnection.ReceiverThread] I will waitAndConnect connect with {}", remoteId);
                    waitAndConnect();
                }
            }
        }
    }

    //******* EDUARDO BEGIN: special thread for receiving messages indicating the entrance into the system, coming from the TTP **************//
    // Simly pass the messages to the replica, indicating its entry into the system
    //TODO: Ask eduardo why a new thread is needed!!! 
    //TODO2: Remove all duplicated code

    /**
     * Thread used to receive packets from the remote server.
     */
    protected class TTPReceiverThread extends Thread {

        private ServiceReplica replica;

        public TTPReceiverThread(ServiceReplica replica) {
            super("TTPReceiver for " + remoteId);
            this.replica = replica;
        }

        @Override
        public void run() {
            byte[] receivedMac = null;
            try {
                receivedMac = new byte[Mac.getInstance(MAC_ALGORITHM).getMacLength()];
            } catch (NoSuchAlgorithmException ex) {
            }

            while (doWork) {
                if (socket != null && socketInStream != null) {
                    try {
                        //read data length
                        int dataLength = socketInStream.readInt();

                        byte[] data = new byte[dataLength];

                        //read data
                        int read = 0;
                        do {
                            read += socketInStream.read(data, read, dataLength - read);
                        } while (read < dataLength);

                        //read mac
                        boolean result = true;
                        
                        byte hasMAC = socketInStream.readByte();
                        if (controller.getStaticConf().getUseMACs() == 1 && hasMAC == 1) {
                            
                            LOGGER.debug("TTP CON USEMAC");
                            read = 0;
                            do {
                                read += socketInStream.read(receivedMac, read, macSize - read);
                            } while (read < macSize);

                            result = Arrays.equals(macReceive.doFinal(data), receivedMac);
                        }

                        if (result) {
                            SystemMessage sm = (SystemMessage) (new ObjectInputStream(new ByteArrayInputStream(data)).readObject());

                            if (sm.getSender() == remoteId) {
                                //LOGGER.debug("Mensagem recebia de: "+remoteId);
                                /*if (!inQueue.offer(sm)) {
                                bftsmart.tom.util.LOGGER.debug("(ReceiverThread.run) in queue full (message from " + remoteId + " discarded).");
                                LOGGER.debug("(ReceiverThread.run) in queue full (message from " + remoteId + " discarded).");
                                }*/
                                this.replica.joinMsgReceived((VMMessage) sm);
                            }
                        } else {
                            //TODO: violation of authentication... we should do something
                            LOGGER.warn("WARNING: Violation of authentication in message received from {}", remoteId);
                        }
                    } catch (ClassNotFoundException ex) {
                        ex.printStackTrace();
                    } catch (IOException ex) {
                        //ex.printStackTrace();
                        if (doWork) {
                            LOGGER.error("[ServerConnection.TTPReceiverThread] I will close socket and waitAndConnect connect with {}", remoteId);
                            closeSocket();
                            waitAndConnect();
                        }
                    }
                } else {
                    LOGGER.error("[ServerConnection.TTPReceiverThread] I will waitAndConnect connect with {}", remoteId);
                    waitAndConnect();
                }
            }
        }
    }
        //******* EDUARDO END **************//
}
