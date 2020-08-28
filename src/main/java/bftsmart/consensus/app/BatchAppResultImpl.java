package bftsmart.consensus.app;

import java.util.List;

/**
 * 预结块结果，包括：交易的响应列表集合， 预计产生新区块的哈希字节数组
 *
 * @author zhangshuang
 *
 */
public class BatchAppResultImpl implements BatchAppResult {

    private List<byte[]> asyncResponses;
    private byte[] blockHashBytes;
    private byte[] genisHashBytes;
    private String batchId;
    private byte computeCode;

    public BatchAppResultImpl(List<byte[]> asyncResponses, byte[] blockHashBytes, String batchId, byte[] genisHashBytes) {
        this.blockHashBytes = blockHashBytes;
        this.batchId = batchId;
        this.asyncResponses = asyncResponses;
        this.genisHashBytes = genisHashBytes;
    }

    @Override
    public List<byte[]> getAsyncResponses() {
        return asyncResponses;
    }

    @Override
    public byte[] getAppHashBytes() {
        return blockHashBytes;
    }

    @Override
    public  String getBatchId() {
        return batchId;
    }

    @Override
    public byte getComputeCode() {
        return computeCode;
    }

    @Override
    public byte[] getGenisHashBytes() {
        return genisHashBytes;
    }

    public void setAsyncResponses(List<byte[]> asyncResponses) {
        this.asyncResponses = asyncResponses;
    }

    public byte[] getBlockHashBytes() {
        return blockHashBytes;
    }

    public void setBlockHashBytes(byte[] blockHashBytes) {
        this.blockHashBytes = blockHashBytes;
    }

    public void setGenisHashBytes(byte[] genisHashBytes) {
        this.genisHashBytes = genisHashBytes;
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }

    public void setComputeCode(byte computeCode) {
        this.computeCode = computeCode;
    }

    public void setComputeCode(ComputeCode computeCode) {
        setComputeCode(computeCode.getCode());
    }

    public static BatchAppResultImpl createSuccess(List<byte[]> responseLinkedList, byte[] blockHashBytes, String batchId, byte[] genisHashBytes) {
        return create(responseLinkedList, blockHashBytes, batchId, genisHashBytes, ComputeCode.SUCCESS);
    }

    public static BatchAppResultImpl createFailure(List<byte[]> responseLinkedList, byte[] blockHashBytes, String batchId, byte[] genisHashBytes) {
        return create(responseLinkedList, blockHashBytes, batchId, genisHashBytes, ComputeCode.FAILURE);
    }

    private static BatchAppResultImpl create(List<byte[]> responseLinkedList, byte[] blockHashBytes, String batchId, byte[] genisHashBytes, ComputeCode computeCode) {
        BatchAppResultImpl batchAppResult = new BatchAppResultImpl(responseLinkedList, blockHashBytes, batchId, genisHashBytes);
        batchAppResult.setComputeCode(computeCode);
        return batchAppResult;
    }
}
