/**
Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package bftsmart.reconfiguration.util;

import bftsmart.reconfiguration.views.NodeNetwork;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

public class HostsConfig implements Serializable {

	private static final long serialVersionUID = -5947185919560079460L;

	private Map<Integer, Config> servers = new ConcurrentHashMap<>();

	public HostsConfig() {
	}
	
	public HostsConfig(String hostsConfigFile) {
		loadConfig(hostsConfigFile);
	}
	
    //create constructor according to consensus realm nodes
	public HostsConfig(Config[] nodesConfig) {
		for (Config node : nodesConfig) {
			add(node.id, node.host, node.consensusPort, node.monitorPort);
		}
	}

//	private void loadConfig(String configHome, String fileName) {
	private void loadConfig(String hostsConfigFile) {
		try {
//			String path = "";
//			String sep = System.getProperty("file.separator");
//			if (configHome.equals("")) {
//				if (fileName.equals(""))
//					path = "config" + sep + "hosts.config";
//				else
//					path = "config" + sep + fileName;
//			} else {
//				if (fileName.equals(""))
//					path = configHome + sep + "hosts.config";
//				else
//					path = configHome + sep + fileName;
//			}
			FileReader fr = new FileReader(hostsConfigFile);
			BufferedReader rd = new BufferedReader(fr);
			String line = null;
			while ((line = rd.readLine()) != null) {
				if (!line.startsWith("#")) {
					StringTokenizer str = new StringTokenizer(line, " ");
					if (str.countTokens() > 2) {
						int id = Integer.valueOf(str.nextToken());
						String host = str.nextToken();
						int consensusPort = Integer.valueOf(str.nextToken());
						try {
							int monitorPort = Integer.valueOf(str.nextToken());
							this.servers.put(id, new Config(id, host, consensusPort, monitorPort));
						} catch (Exception e) {
							this.servers.put(id, new Config(id, host, consensusPort, -1));
						}
					}
				}
			}
			fr.close();
			rd.close();
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
	}

	public void add(int id, String host, int consensusPort, int monitorPort) {
		this.servers.put(id, new Config(id, host, consensusPort, monitorPort));
	}

	public int getNum() {
		return servers.size();
	}

	public NodeNetwork getRemoteAddress(int id) {
		Config c = (Config) this.servers.get(id);
		if (c != null) {
			return new NodeNetwork(c.host, c.consensusPort, c.monitorPort);
		}
		return null;
	}

	public NodeNetwork getServerToServerRemoteAddress(int id) {
		Config c = (Config) this.servers.get(id);
		if (c != null) {
			return new NodeNetwork(c.host, c.consensusPort + 1, c.monitorPort);
		}
		return null;
	}

	public int getPort(int id) {
		Config c = (Config) this.servers.get(id);
		if (c != null) {
			return c.consensusPort;
		}
		return -1;
	}

	public int getMonitorPort(int id) {
		Config c = (Config) this.servers.get(id);
		if (c != null) {
			return c.monitorPort;
		}
		return -1;
	}

	public int getServerToServerPort(int id) {
		Config c = (Config) this.servers.get(id);
		if (c != null) {
			return c.consensusPort + 1;
		}
		return -1;
	}

	public int[] getHostsIds() {
		Set<Integer> s = this.servers.keySet();
		int[] ret = new int[s.size()];
		Iterator<Integer> it = s.iterator();
		int p = 0;
		while (it.hasNext()) {
			ret[p] = Integer.parseInt(it.next().toString());
			p++;
		}
		return ret;
	}

	public String getHost(int id) {
		Config c = (Config) this.servers.get(id);
		if (c != null) {
			return c.host;
		}
		return null;
	}

	public NodeNetwork getLocalAddress(int id) {
		Config c = (Config) this.servers.get(id);
		if (c != null) {
			return new NodeNetwork(c.host, c.consensusPort, c.monitorPort);
		}
		return null;
	}

	public static class Config implements Serializable {
		private static final long serialVersionUID = -7986629371931246948L;

		private int id;
		private String host;
		private int consensusPort;
		private int monitorPort;

		public Config(int id, String host, int consensusPort, int monitorPort) {
			this.id = id;
			this.host = host;
			this.consensusPort = consensusPort;
			this.monitorPort = monitorPort;
		}
	}
}
