package bftsmart.reconfiguration;

public interface ReplicaTopology extends ViewTopology{

	int[] getCurrentViewOtherAcceptors();

	int[] getCurrentViewAcceptors();

	boolean isInLastJoinSet(int id);

	int getQuorum();

}