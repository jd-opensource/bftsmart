package test.bftsmart.leaderchange;

import bftsmart.communication.MessageHandler;
import bftsmart.communication.ServerCommunicationSystem;
import bftsmart.communication.ServerCommunicationSystemImpl;
import bftsmart.communication.client.ClientCommunicationFactory;
import bftsmart.communication.client.ClientCommunicationServerSide;
import bftsmart.consensus.app.BatchAppResultImpl;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.reconfiguration.util.HostsConfig;
import bftsmart.reconfiguration.util.TOMConfiguration;
import bftsmart.reconfiguration.views.MemoryBasedViewStorage;
import bftsmart.reconfiguration.views.View;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ReplyContextMessage;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultRecoverable;
import bftsmart.tom.util.BytesUtils;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @Author: zhangshuang
 * @Date: 2020/3/4 11:50 AM
 * Version 1.0
 */
public class TestNodeServer extends DefaultRecoverable {

    private int counter = 0;

    private int iterations = 0;

    private ServiceReplica replica = null;

    private int proId;

    private View latestView;

    private Properties systemConfig;

    private HostsConfig hostsConfig;

    public TestNodeServer(int id, View latestView, Properties systemConfig, HostsConfig hostsConfig) {

        this.proId = id;

        this.latestView = latestView;

        this.systemConfig = systemConfig;

        this.hostsConfig = hostsConfig;
    }

    private TOMConfiguration initConfig() {
        try {
            Properties sysConfClone = (Properties) systemConfig.clone();
            return new TOMConfiguration(proId, sysConfClone, hostsConfig);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException("Config file resolve error!");
        }
    }

    public ServiceReplica startNode(String realmName) {

    	TOMConfiguration config  = initConfig();

    	try {
            // mock messsageHandler and cs
            MessageHandler messageHandler = new MessageHandler();
            MessageHandler mockMessageHandler = Mockito.spy(messageHandler);

            ClientCommunicationServerSide clientCommunication = ClientCommunicationFactory.createServerSide(new ServerViewController(config, new MemoryBasedViewStorage(latestView)));
            ServerCommunicationSystem cs = new ServerCommunicationSystemImpl(clientCommunication, mockMessageHandler, new ServerViewController(config, new MemoryBasedViewStorage(latestView)),
                    realmName);

            ServerCommunicationSystem mockCs = Mockito.spy(cs);

            replica = new ServiceReplica(mockMessageHandler, mockCs, config, this, this,
                    (int) -1, latestView, realmName);
        } catch (Exception e) {
    	    e.printStackTrace();
        }

        return replica;
    }

    public ServiceReplica getReplica() {
        return replica;
    }

    @Override
    public byte[][] appExecuteBatch(byte[][] commands, MessageContext[] msgCtxs, boolean fromConsensus) {

        byte [][] replies = new byte[commands.length][];
        for (int i = 0; i < commands.length; i++) {
            if(msgCtxs != null && msgCtxs[i] != null) {
                replies[i] = executeSingle(commands[i],msgCtxs[i]);
            }
            else executeSingle(commands[i],null);
        }

        return replies;
    }

//    @Override
//    public byte[][] appExecuteBatch(byte[][] commands, MessageContext[] msgCtxs, boolean fromConsensus) {
//        return appExecuteBatch(commands, msgCtxs, fromConsensus);
//    }

    @Override
    public byte[] appExecuteUnordered(byte[] command, MessageContext msgCtx) {

        iterations++;
        System.out.println("(" + iterations + ") Reading counter at value: " + counter);
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream(4);
            new DataOutputStream(out).writeInt(counter);
            return out.toByteArray();
        } catch (IOException ex) {
            System.err.println("Invalid request received!");
            return new byte[0];
        }
    }

    @Override
    public int getCommandsNumByCid(int cid) {
        return 0;
    }

    @Override
    public byte[][] getCommandsByCid(int cid, int currCidCommandsNum) {
        return new byte[0][];
    }

    @Override
    public long getTimestampByCid(int cid) {
        return 0;
    }

    private byte[] executeSingle(byte[] command, MessageContext msgCtx) {
        iterations++;
        try {
            int increment = new DataInputStream(new ByteArrayInputStream(command)).readInt();
            //System.out.println("read-only request: "+(msgCtx.getConsensusId() == -1));
            counter++;

            if (msgCtx != null) {
                if (msgCtx.getConsensusId() == -1) {
                    System.out.println("(" + iterations + ") Counter was incremented: " + counter);
                } else {
//                    System.out.println("Procid " + proId + " (" + iterations + " / " + msgCtx.getConsensusId() + ") Counter was incremented: " + counter);
                }
            }
            else {
                System.out.println("(" + iterations + ") Counter was incremented: " + counter);
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream(4);
            new DataOutputStream(out).writeInt(counter);
            return out.toByteArray();
        } catch (IOException ex) {
            System.err.println("Invalid request received!");
            return new byte[0];
        }
    }

//    public static void main(String[] args){
//        if(args.length < 2) {
//            System.out.println("Use: java CounterServer <processId>");
//            System.exit(-1);
//        }
//        new TestNodeServer(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
//    }


    @SuppressWarnings("unchecked")
    @Override
    public void installSnapshot(byte[] state) {
        try {
            System.out.println("setState called");
            ByteArrayInputStream bis = new ByteArrayInputStream(state);
            ObjectInput in = new ObjectInputStream(bis);
            counter =  in.readInt();
            in.close();
            bis.close();
        } catch (Exception e) {
            System.err.println("[ERROR] Error deserializing state: "
                    + e.getMessage());
        }
    }

    @Override
    public byte[] getBlockHashByCid(int cid) {
        try {
            System.out.println("getState called");
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeInt(counter);
            out.flush();
            bos.flush();
            out.close();
            bos.close();
            return bos.toByteArray();
        } catch (IOException ioe) {
            System.err.println("[ERROR] Error serializing state: "
                    + ioe.getMessage());
//			return "ERROR".getBytes();
            return BytesUtils.getBytes("ERROR");
        }
    }

    @Override
    public BatchAppResultImpl preComputeAppHash(int cid, byte[][] commands, long timestamp) {
        List<byte[]> responseLinkedList = new ArrayList<>();

        for (int i = 0; i < commands.length; i++) {
            responseLinkedList.add("test".getBytes());
        }

        return new BatchAppResultImpl(responseLinkedList, "new".getBytes(), "batchId", "genis".getBytes() );

    }

    @Override
    public List<byte[]> updateAppResponses(List<byte[]> asyncResponseLinkedList, byte[] commonHash, boolean isConsistent) {
        return null;
    }

    @Override
    public void preComputeAppCommit(int cid, String batchId) {
        return;
    }

    @Override
    public void preComputeAppRollback(int cid, String batchId) {
        return;
    }
}
