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
    public BatchAppResultImpl preComputeHash(byte[][] command);

    public List<byte[]> updateResponses(List<byte[]> asyncResponseLinkedList, byte[] commonHash, boolean isConsistent);

    // batch commit
    public void preComputeCommit(String batchId);

    // batch rollback
    public void preComputeRollback(String batchId);

    // batch exe new interface
    public byte[][] executeBatch(byte[][] command, MessageContext[] msgCtx, List<ReplyContextMessage> replyContextMessages);

}
