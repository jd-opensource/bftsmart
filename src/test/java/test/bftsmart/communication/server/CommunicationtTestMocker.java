package test.bftsmart.communication.server;

import static org.mockito.Mockito.when;

import java.util.Properties;

import org.mockito.Mockito;

import bftsmart.reconfiguration.ReplicaTopology;
import bftsmart.reconfiguration.ViewTopology;
import bftsmart.reconfiguration.util.DefaultRSAKeyLoader;
import bftsmart.reconfiguration.util.HostsConfig;
import bftsmart.reconfiguration.util.TOMConfiguration;
import bftsmart.tom.ReplicaConfiguration;

public class CommunicationtTestMocker {

	public static ViewTopology mockTopology(int currentId, int[] processIds) {
		ViewTopology topology = Mockito.mock(ViewTopology.class);
		when(topology.getCurrentProcessId()).thenReturn(currentId);
		when(topology.getCurrentViewProcesses()).thenReturn(processIds);
		when(topology.isInCurrentView()).thenReturn(contains(currentId, processIds));

		ReplicaConfiguration conf = mockConfiguration(currentId, processIds);
		when(topology.getStaticConf()).thenReturn(conf);

		return topology;
	}

	public static ReplicaTopology mockTopologyWithTCP(int currentId, int[] processIds, int[] ports) {
		ReplicaTopology topology = Mockito.mock(ReplicaTopology.class);
		when(topology.getCurrentProcessId()).thenReturn(currentId);
		when(topology.getCurrentViewProcesses()).thenReturn(processIds);
		when(topology.isInCurrentView()).thenReturn(contains(currentId, processIds));

		ReplicaConfiguration conf = mockConfiguration(currentId, processIds, ports);
		when(topology.getStaticConf()).thenReturn(conf);

		return topology;
	}

	public static ReplicaConfiguration mockConfiguration(int currentId, int[] processIds) {
		ReplicaConfiguration conf = Mockito.mock(ReplicaConfiguration.class);
		when(conf.getProcessId()).thenReturn(currentId);
		when(conf.getInitialView()).thenReturn(processIds);
		when(conf.getInQueueSize()).thenReturn(100000);
		when(conf.getOutQueueSize()).thenReturn(100000);
		when(conf.getSendRetryCount()).thenReturn(10);

		try {
			DefaultRSAKeyLoader defaultRSAKeyLoader = new DefaultRSAKeyLoader();
			when(conf.getRSAPrivateKey()).thenReturn(defaultRSAKeyLoader.loadPrivateKey(currentId));
			when(conf.getRSAPublicKey(currentId)).thenReturn(defaultRSAKeyLoader.loadPublicKey(currentId));
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
		return conf;
	}

	public static ReplicaConfiguration mockConfiguration(int currentId, int[] processIds, int[] ports) {
		HostsConfig hosts = new HostsConfig();
		for (int i = 0; i < processIds.length; i++) {
			hosts.add(processIds[i], "localhost", ports[i], 0);
		}
		TOMConfiguration conf = new TOMConfiguration(currentId, new Properties(), hosts);
		
		conf = Mockito.spy(conf);
		
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

}
