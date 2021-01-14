package bftsmart.reconfiguration;

import java.net.InetSocketAddress;

import bftsmart.reconfiguration.views.NodeNetwork;
import bftsmart.reconfiguration.views.View;
import bftsmart.tom.ReplicaConfiguration;

public interface ViewTopology {

	int getCurrentProcessId();

	View getCurrentView();

	View getLastView();

	NodeNetwork getRemoteAddress(int id);

	InetSocketAddress getRemoteSocketAddress(int id);

	ReplicaConfiguration getStaticConf();

	boolean isCurrentViewMember(int id);

	int getCurrentViewId();

	int getCurrentViewF();

	int getCurrentViewN();

	int getCurrentViewPos(int id);

	int[] getCurrentViewProcesses();

	default boolean isInCurrentView() {
		return isCurrentViewMember(getCurrentProcessId());
	}

}