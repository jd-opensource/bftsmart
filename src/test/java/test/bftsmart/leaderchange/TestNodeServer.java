package test.bftsmart.leaderchange;

import bftsmart.consensus.app.BatchAppResultImpl;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ReplyContextMessage;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.leaderchange.HeartBeatTimer;
import bftsmart.tom.server.defaultservices.DefaultRecoverable;
import bftsmart.tom.server.defaultservices.DefaultReplier;
import bftsmart.tom.util.BytesUtils;
import org.mockito.Mockito;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: zhangshuang
 * @Date: 2020/3/4 11:50 AM
 * Version 1.0
 */
public class TestNodeServer extends DefaultRecoverable {

    private int counter = 0;

    private int iterations = 0;

    ServiceReplica replica = null;

    private int proId;

    public TestNodeServer(int id) {
        this.proId = id;
    }

    public ServiceReplica startNode() {

        replica = new ServiceReplica(proId, "config", this, this, null, new DefaultReplier());
//        replica = new ServiceReplica(proId, "config", this, this, null, new DefaultReplier(), Mockito.spy(new HeartBeatTimer()));
        return replica;
    }

    public ServiceReplica getReplica() {
        return this.replica;
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

    @Override
    public byte[][] appExecuteBatch(byte[][] commands, MessageContext[] msgCtxs, boolean fromConsensus, List<ReplyContextMessage> replyContextMessages) {
        return appExecuteBatch(commands, msgCtxs, fromConsensus);
    }

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

    public static void main(String[] args){
        if(args.length < 1) {
            System.out.println("Use: java CounterServer <processId>");
            System.exit(-1);
        }
        new TestNodeServer(Integer.parseInt(args[0]));
    }


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
    public byte[] getSnapshot() {
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
    public BatchAppResultImpl preComputeAppHash(int cid, byte[][] commands) {
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
