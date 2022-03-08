package bftsmart.consensus.app;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ReplyContextMessage;
import bftsmart.tom.server.Executable;

import java.util.List;


/**
 *
 *
 * provide new interface for app
 *
 *
 */
public interface PreComputeBatchExecutable extends Executable {

    // begin batch, process order, complete batch
    public BatchAppResultImpl preComputeHash(int cid, byte[][] command, long timestamp);

    public List<byte[]> updateResponses(List<byte[]> asyncResponseLinkedList, byte[] commonHash, boolean isConsistent);

    // batch commit
    public void preComputeCommit(int cid, String batchId);

    // batch rollback
    public void preComputeRollback(int cid, String batchId);

    // batch exe new interface
    public byte[][] executeBatch(byte[][] command, MessageContext[] msgCtx);

}
