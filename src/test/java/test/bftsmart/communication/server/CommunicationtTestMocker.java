package test.bftsmart.communication.server;

import static org.mockito.Mockito.*;

import java.util.Properties;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import bftsmart.reconfiguration.ReplicaTopology;
import bftsmart.reconfiguration.ViewTopology;
import bftsmart.reconfiguration.util.DefaultRSAKeyLoader;
import bftsmart.reconfiguration.util.HostsConfig;
import bftsmart.reconfiguration.util.TOMConfiguration;
import bftsmart.tom.ReplicaConfiguration;

public class CommunicationtTestMocker {

	public static ViewTopology mockTopology(int currentId, int[] processIds) {
		ReplicaConfiguration conf = mockConfiguration(currentId, processIds);

		return mockTopology(currentId, processIds, conf);
	}

	public static ViewTopology mockTopology(int currentId, int[] processIds, ReplicaConfiguration conf) {
		ViewTopology topology = Mockito.mock(ViewTopology.class);
		when(topology.getCurrentProcessId()).thenReturn(currentId);
		when(topology.getCurrentViewProcesses()).thenReturn(processIds);
		when(topology.isInCurrentView()).thenReturn(contains(currentId, processIds));
		when(topology.getStaticConf()).thenReturn(conf);

		return topology;
	}

	public static ReplicaTopology mockTopologyWithTCP(int currentId, int[] processIds, int[] ports) {
		ReplicaTopology topology = Mockito.mock(ReplicaTopology.class);
		when(topology.getCurrentProcessId()).thenReturn(currentId);
		when(topology.getCurrentViewProcesses()).thenReturn(processIds);
		when(topology.isInCurrentView()).thenReturn(contains(currentId, processIds));
		when(topology.isCurrentViewMember(anyInt())).thenAnswer(new Answer<Boolean>() {
			@Override
			public Boolean answer(InvocationOnMock invocation) throws Throwable {
				int id = invocation.getArgument(0);
				return contains(id, processIds);
			}
		});

		ReplicaConfiguration conf = mockDefaultConfiguration(currentId, processIds, ports);
		when(topology.getStaticConf()).thenReturn(conf);

		return topology;
	}

	public static ReplicaConfiguration mockConfiguration(int currentId, int[] processIds) {
		ReplicaConfiguration conf = Mockito.mock(ReplicaConfiguration.class);
		when(conf.getInQueueSize()).thenReturn(100000);
		try {
			DefaultRSAKeyLoader defaultRSAKeyLoader = new DefaultRSAKeyLoader();
			} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
		return conf;
	}

	public static ReplicaConfiguration mockDefaultConfiguration(int currentId, int[] processIds) {
		return mockDefaultConfiguration(currentId, processIds, null);
	}
	public static ReplicaConfiguration mockDefaultConfiguration(int currentId, int[] processIds, int[] ports) {
		HostsConfig hosts = new HostsConfig();
		for (int i = 0; i < processIds.length; i++) {
			int port = ports == null ? 0 : ports[i];
			hosts.add(processIds[i], "localhost", port, 0);
		}
		TOMConfiguration conf = new TOMConfiguration(currentId, new Properties(), hosts);

		conf = Mockito.spy(conf);

		when(conf.getProcessId()).thenReturn(currentId);
		when(conf.getInQueueSize()).thenReturn(100000);
		when(conf.getOutQueueSize()).thenReturn(100000);

		return conf;

//		ReplicaConfiguration conf = mockConfiguration(currentId, processIds);
//
//		when(conf.getServerToServerPort(anyInt())).thenAnswer(new Answer<Integer>() {
//			@Override
//			public Integer answer(InvocationOnMock invocation) throws Throwable {
//				int id = invocation.getArgument(0);
//				for (int i = 0; i < processIds.length; i++) {
//					if (processIds[i] == id) {
//						return ports[i];
//					}
//				}
//				throw new IllegalArgumentException(
//						"The id[" + id + "] is out of range[" + Arrays.toString(processIds) + "]!");
//			}
//		});
//		return conf;
	}

	private static boolean contains(int v, int[] values) {
		for (int value : values) {
			if (v == value) {
				return true;
			}
		}
		return false;
	}

	public static ReplicaTopology mockTopologyWithTCP2(int currentId, int[] processIds, int[] ports) {
		ReplicaTopology topology = Mockito.mock(ReplicaTopology.class);
		when(topology.getCurrentProcessId()).thenReturn(currentId);
		when(topology.getCurrentViewProcesses()).thenReturn(processIds);
		when(topology.isInCurrentView()).thenReturn(contains(currentId, processIds));

		ReplicaConfiguration conf = mockDefaultConfiguration(currentId, processIds, ports);
		when(topology.getStaticConf()).thenReturn(conf);

		return topology;
	}

}
