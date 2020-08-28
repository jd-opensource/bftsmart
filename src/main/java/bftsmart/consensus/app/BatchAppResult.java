package bftsmart.consensus.app;


import java.util.List;

public interface BatchAppResult {

    List<byte[]> getAsyncResponses();

    byte[] getAppHashBytes();

    String getBatchId();

    byte getComputeCode();

    byte[] getGenisHashBytes();

}
