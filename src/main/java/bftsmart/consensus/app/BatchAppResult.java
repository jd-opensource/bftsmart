package bftsmart.consensus.app;


import java.util.List;

public interface BatchAppResult {

    public List<byte[]> getAsyncResponseLinkedList();

    public byte[] getAppHashBytes();

    public String getBatchId();

    public byte getErrprCode();

    public byte[] getGenisHashBytes();

}
