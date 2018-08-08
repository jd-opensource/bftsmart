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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.StringTokenizer;

public class Configuration implements Serializable{

	private static final long serialVersionUID = -719578198150380571L;
	
	protected int processId;
	protected boolean channelsBlocking;
	protected BigInteger DH_P;
	protected BigInteger DH_G;
	protected int autoConnectLimit;
	protected Properties systemConfig;
	protected HostsConfig hostsConfig;

	private String hmacAlgorithm = "HmacSha1";
	private int hmacSize = 160;

	// protected static String configHome = "";
	//
	// protected static String hostsFileName = "";

	protected boolean defaultKeys = false;

	public Configuration(int procId) {
		this(procId, "config/system.config", "config/hosts.config");
//		processId = procId;
//		init();
	}

//	public Configuration(int processId, String configHomeParam) {
//		this(processId, configHomeParam, "");
//	}

	public Configuration(int processId, String configHomeParam, String hostsFileNameParam) {
		this.processId = processId;
		init(configHomeParam, hostsFileNameParam);
//		configHome = configHomeParam;
//		hostsFileName = hostsFileNameParam;
//		init();
	}

	public Configuration(int processId, Properties systemConfigs, HostsConfig hostsConfig) {
		this.processId = processId;
		this.systemConfig = systemConfigs;
		this.hostsConfig = hostsConfig;
		init(hostsConfig, systemConfigs);
	}

	// protected void init() {
	// hosts = new HostsConfig(configHome, hostsFileName);
	// loadConfig();
	// init(hosts, configs);
	// }

	protected void init(String systemConfigFile, String hostsConfigFile) {
		hostsConfig = new HostsConfig(hostsConfigFile);
		systemConfig = loadSystemConfig(systemConfigFile);
//		loadConfig(hostsFileName);
		init(hostsConfig, systemConfig);
	}

	protected void init(HostsConfig hosts, Properties configs) {
		try {
			// hosts = new HostsConfig(configHome, hostsFileName);
			//
			// loadConfig();

			String s = (String) configs.remove("system.autoconnect");
			if (s == null) {
				autoConnectLimit = -1;
			} else {
				autoConnectLimit = Integer.parseInt(s);
			}

			s = (String) configs.remove("system.channels.blocking");
			if (s == null) {
				channelsBlocking = false;
			} else {
				channelsBlocking = (s.equalsIgnoreCase("true")) ? true : false;
			}

			s = (String) configs.remove("system.communication.defaultkeys");
			if (s == null) {
				defaultKeys = false;
			} else {
				defaultKeys = (s.equalsIgnoreCase("true")) ? true : false;
			}

			s = (String) configs.remove("system.diffie-hellman.p");
			if (s == null) {
				String pHexString = "FFFFFFFF FFFFFFFF C90FDAA2 2168C234 C4C6628B 80DC1CD1"
						+ "29024E08 8A67CC74 020BBEA6 3B139B22 514A0879 8E3404DD"
						+ "EF9519B3 CD3A431B 302B0A6D F25F1437 4FE1356D 6D51C245"
						+ "E485B576 625E7EC6 F44C42E9 A637ED6B 0BFF5CB6 F406B7ED"
						+ "EE386BFB 5A899FA5 AE9F2411 7C4B1FE6 49286651 ECE65381" + "FFFFFFFF FFFFFFFF";
				DH_P = new BigInteger(pHexString.replaceAll(" ", ""), 16);
			} else {
				DH_P = new BigInteger(s, 16);
			}
			s = (String) configs.remove("system.diffie-hellman.g");
			if (s == null) {
				DH_G = new BigInteger("2");
			} else {
				DH_G = new BigInteger(s);
			}

		} catch (Exception e) {
//			System.err.println("Wrong system.config file format.");
//			e.printStackTrace(System.out);
			
			throw new IllegalArgumentException("Wrong system.config file format! --" + e.getMessage(), e);
		}
	}
	
	
	public Properties getConfigProperties() {
		Properties configs = new Properties();
		
		configs.setProperty("system.autoconnect", autoConnectLimit+"");
		
		configs.setProperty("system.channels.blocking", Boolean.toString(channelsBlocking));
		
		configs.setProperty("system.communication.defaultkeys",  Boolean.toString(defaultKeys));
		
		configs.setProperty("system.diffie-hellman.p", DH_P.toString(16));
		
		configs.setProperty("system.diffie-hellman.g", DH_G.toString());
		
		return configs;
	}

	public boolean useDefaultKeys() {
		return defaultKeys;
	}

	public final boolean isHostSetted(int id) {
		if (hostsConfig.getHost(id) == null) {
			return false;
		}
		return true;
	}

	public final boolean useBlockingChannels() {
		return this.channelsBlocking;
	}

	public final int getAutoConnectLimit() {
		return this.autoConnectLimit;
	}

	public final BigInteger getDHP() {
		return DH_P;
	}

	public final BigInteger getDHG() {
		return DH_G;
	}

	public final String getHmacAlgorithm() {
		return hmacAlgorithm;
	}

	public final int getHmacSize() {
		return hmacSize;
	}

//	public final String getProperty(String key) {
//		Object o = systemConfig.get(key);
//		if (o != null) {
//			return o.toString();
//		}
//		return null;
//	}

//	public final Properties getProperties() {
//		return systemConfig;
//	}

	public final InetSocketAddress getRemoteAddress(int id) {
		return hostsConfig.getRemoteAddress(id);
	}

	public final InetSocketAddress getServerToServerRemoteAddress(int id) {
		return hostsConfig.getServerToServerRemoteAddress(id);
	}

	public final InetSocketAddress getLocalAddress(int id) {
		return hostsConfig.getLocalAddress(id);
	}

	public final String getHost(int id) {
		return hostsConfig.getHost(id);
	}

	public final int getPort(int id) {
		return hostsConfig.getPort(id);
	}

	public final int getServerToServerPort(int id) {
		return hostsConfig.getServerToServerPort(id);
	}

	public final int getProcessId() {
		return processId;
	}

	public final void setProcessId(int processId) {
		this.processId = processId;
	}

	public final void addHostInfo(int id, String host, int port) {
		this.hostsConfig.add(id, host, port);
	}

	
//	private Properties loadSystemConfig(String configHome) {
	
	private Properties loadSystemConfig(String systemConfigFile) {
		try {
			Properties systemConfig = new Properties();
//			if (configHome == null || configHome.equals("")) {
//				configHome = "config";
//			}
//			String sep = System.getProperty("file.separator");
//			String path = configHome + sep + "system.config";
			try (FileReader fr = new FileReader(systemConfigFile); BufferedReader rd = new BufferedReader(fr);) {
				String line = null;
				while ((line = rd.readLine()) != null) {
					if (!line.startsWith("#")) {
						StringTokenizer str = new StringTokenizer(line, "=");
						if (str.countTokens() > 1) {
							systemConfig.setProperty(str.nextToken().trim(), str.nextToken().trim());
						}
					}
				}
			}
			return systemConfig;
		} catch (IOException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}
}
