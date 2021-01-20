package bftsmart.reconfiguration;

import bftsmart.reconfiguration.views.View;

public interface ReplicaTopology extends ViewTopology{

	int[] getCurrentViewOtherAcceptors();

	boolean isInLastJoinSet(int id);

	int getQuorum();

	
	@Deprecated
	void addHostInfo(int procId, String host, int consensusPort, int monitorPort);

	@Deprecated
	void reconfigureTo(View currentView);

}