package bftsmart.consensus.app;

import java.util.ArrayList;
import java.util.List;

/**
 * 预结块结果，包括：交易的响应列表集合， 预计产生新区块的哈希字节数组
 *
 * @author zhangshuang
 *
 */
public class BatchAppResultImpl implements BatchAppResult {

    private List<byte[]> asyncResponseLinkedList = new ArrayList<>();
    private byte[] blockHashBytes;
    private String batchId;

    public BatchAppResultImpl(List<byte[]> reponseLinkedList, byte[] blockHashBytes, String batchId) {
        this.blockHashBytes = blockHashBytes;
        this.batchId = batchId;
        for(int i = 0; i < reponseLinkedList.size(); i++) {
            asyncResponseLinkedList.add(reponseLinkedList.get(i));
        }
    }

    @Override
    public List<byte[]> getAsyncResponseLinkedList() {
        return asyncResponseLinkedList;
    }

    @Override
    public byte[] getAppHashBytes() {
        return blockHashBytes;
    }

    @Override
    public  String getBatchId() {
        return batchId;
    }

    public void setAsyncResponseLinkedList(List<byte[]> responseLinkedList) {
        for(int i = 0; i < responseLinkedList.size(); i++) {
            asyncResponseLinkedList.add(responseLinkedList.get(i));
        }
    }

    public void setBlockHashBytes(byte[] blockHashBytes) {
        this.blockHashBytes = blockHashBytes;
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }
}
