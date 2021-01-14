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

import java.io.Serializable;
import java.math.BigInteger;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Properties;
import java.util.StringTokenizer;

import bftsmart.reconfiguration.views.NodeNetwork;
import bftsmart.reconfiguration.views.NullNodeNetwork;
import bftsmart.tom.ReplicaConfiguration;

public class TOMConfiguration implements Serializable, ReplicaConfiguration {

	private static final long serialVersionUID = -8770247165391836152L;
	
	protected int processId;
	
	protected boolean channelsBlocking;
	protected BigInteger DH_P;
	protected BigInteger DH_G;
	protected int autoConnectLimit;
	protected Properties systemConfig;
	protected HostsConfig hostsConfig;

	private String hmacAlgorithm = "HmacSha1";
	private int hmacSize = 160;

	protected boolean defaultKeys = false;

	private volatile int n;
	private volatile int f;
	private int requestTimeout;
	private int clientDatasMonitorTimeout;
	private int clientDatasMaxCount;
	private long heartBeatTimeout;
	private long heartBeatPeriod;
	private int tomPeriod;
	private int paxosHighMark;
	private int revivalHighMark;
	private int timeoutHighMark;
	private int replyVerificationTime;
	private int maxBatchSize;
	private long timeTolerance;
	private int numberOfNonces;
	private int inQueueSize;
	private int outQueueSize;
	private boolean shutdownHookEnabled;
//	private boolean useSenderThread;
	private long sendRetryInterval;
	private int sendRetryCount;
	private RsaKeyLoader rsaLoader;
	private int debug;
	private int numNIOThreads;
	private int useMACs;
	private int useSignatures;
	private boolean stateTransferEnabled;
	private int checkpointPeriod;
	private int globalCheckpointPeriod;
	private int useControlFlow;
	private volatile int[] initialView;
	private int ttpId;
	private boolean isToLog;
	private boolean syncLog;
	private boolean parallelLog;
	private boolean logToDisk;
	private boolean isToWriteCkpsToDisk;
	private boolean syncCkp;
	private boolean isBFT;
	private int numRepliers;
	private int numNettyWorkers;
	private HostsConfig outerHostConfig;

	public TOMConfiguration(int processId, Properties systemConfigs, HostsConfig hostsConfig) {
		this.processId = processId;
		this.systemConfig = systemConfigs;
		this.hostsConfig = hostsConfig;
		initSystemConfig(hostsConfig, systemConfigs);
		initTomConfig(hostsConfig, systemConfigs);

		this.rsaLoader = new DefaultRSAKeyLoader();
	}

	/** Creates a new instance of TOMConfiguration */
	public TOMConfiguration(int processId, Properties systemConfigs, HostsConfig hostConfig,
			HostsConfig outerHostConfig) {
		this(processId, systemConfigs, hostConfig);
		this.outerHostConfig = outerHostConfig;
		this.rsaLoader = new DefaultRSAKeyLoader();
	}

	private void initSystemConfig(HostsConfig hosts, Properties configs) {
		try {
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
			throw new IllegalArgumentException("Wrong system.config file format! --" + e.getMessage(), e);
		}
	}

	private void initTomConfig(HostsConfig hosts, Properties configs) {
		try {
			n = Integer.parseInt(configs.remove("system.servers.num").toString());
			String s = (String) configs.remove("system.servers.f");
			if (s == null) {
				f = (int) Math.ceil((n - 1) / 3);
			} else {
				f = Integer.parseInt(s);
				// add verify by zhangshuang
				if (f != ((int) Math.ceil((n - 1) / 3))) {
					f = (int) Math.ceil((n - 1) / 3);
				}
			}

			s = (String) configs.remove("system.shutdownhook");
			shutdownHookEnabled = (s != null) ? Boolean.parseBoolean(s) : false;

			s = (String) configs.remove("system.totalordermulticast.period");
			if (s == null) {
				tomPeriod = n * 5;
			} else {
				tomPeriod = Integer.parseInt(s);
			}

			s = (String) configs.remove("system.totalordermulticast.timeout");
			if (s == null) {
				requestTimeout = 10000;
			} else {
				requestTimeout = Integer.parseInt(s);
				if (requestTimeout < 0) {
					requestTimeout = 0;
				}
			}

			s = (String) configs.remove("system.totalordermulticast.clientDatasMonitorTimeout");
			if (s == null) {
				clientDatasMonitorTimeout = 20000;
			} else {
				clientDatasMonitorTimeout = Integer.parseInt(s);
				if (clientDatasMonitorTimeout < 0) {
					clientDatasMonitorTimeout = 0;
				}
			}

			s = (String) configs.remove("system.totalordermulticast.clientDatasMaxCount");
			if (s == null) {
				clientDatasMaxCount = 200000;
			} else {
				clientDatasMaxCount = Integer.parseInt(s);
				if (clientDatasMaxCount < 0) {
					clientDatasMaxCount = 0;
				}
			}

			// 时间容错范围，默认为300秒
			s = (String) configs.remove("system.totalordermulticast.timeTolerance");
			if (s == null) {
				timeTolerance = 300000L;
			} else {
				timeTolerance = Long.parseLong(s);
				if (timeTolerance <= 0) {
					timeTolerance = 300000L;
				} else if (timeTolerance < 30000L) {
					timeTolerance = 30000L;
				}
			}

			// heartBeatTimeout;
			s = (String) configs.remove("system.totalordermulticast.heartBeatTimeout");
			if (s == null) {
				heartBeatTimeout = 30000L;
			} else {
				heartBeatTimeout = Long.parseLong(s);
				if (heartBeatTimeout < 0) {
					heartBeatTimeout = 30000L;
				}
			}

			// heartBeatPeriod;
			s = (String) configs.remove("system.totalordermulticast.heartBeatPeriod");
			if (s == null) {
				heartBeatPeriod = 5000L;
			} else {
				heartBeatPeriod = Long.parseLong(s);
				if (heartBeatPeriod < 0) {
					heartBeatPeriod = 5000L;
				}
			}

			s = (String) configs.remove("system.totalordermulticast.highMark");
			if (s == null) {
				paxosHighMark = 10000;
			} else {
				paxosHighMark = Integer.parseInt(s);
				if (paxosHighMark < 10) {
					paxosHighMark = 10;
				}
			}

			s = (String) configs.remove("system.totalordermulticast.revival_highMark");
			if (s == null) {
				revivalHighMark = 10;
			} else {
				revivalHighMark = Integer.parseInt(s);
				if (revivalHighMark < 1) {
					revivalHighMark = 1;
				}
			}

			s = (String) configs.remove("system.totalordermulticast.timeout_highMark");
			if (s == null) {
				timeoutHighMark = 100;
			} else {
				timeoutHighMark = Integer.parseInt(s);
				if (timeoutHighMark < 1) {
					timeoutHighMark = 1;
				}
			}

			s = (String) configs.remove("system.totalordermulticast.maxbatchsize");
			if (s == null) {
				maxBatchSize = 100;
			} else {
				maxBatchSize = Integer.parseInt(s);
			}

			s = (String) configs.remove("system.totalordermulticast.replayVerificationTime");
			if (s == null) {
				replyVerificationTime = 0;
			} else {
				replyVerificationTime = Integer.parseInt(s);
			}

			s = (String) configs.remove("system.totalordermulticast.nonces");
			if (s == null) {
				numberOfNonces = 0;
			} else {
				numberOfNonces = Integer.parseInt(s);
			}

//			s = (String) configs.remove("system.communication.useSenderThread");
//			if (s == null) {
//				useSenderThread = true;
//			} else {
//				useSenderThread = Boolean.parseBoolean(s);
//			}

			s = (String) configs.remove("system.communication.send.retryInterval");
			if (s == null) {
				// 默认2000；
				sendRetryInterval = 2000;
			} else {
				sendRetryInterval = Long.parseLong(s);
			}

			s = (String) configs.remove("system.communication.send.retryCount");
			if (s == null) {
				// 默认2000；
				sendRetryCount = 100;
			} else {
				sendRetryCount = Integer.parseInt(s);
			}

			s = (String) configs.remove("system.communication.numNIOThreads");
			if (s == null) {
				numNIOThreads = 2;
			} else {
				numNIOThreads = Integer.parseInt(s);
			}

			s = (String) configs.remove("system.communication.useMACs");
			if (s == null) {
				useMACs = 0;
			} else {
				useMACs = Integer.parseInt(s);
			}

			s = (String) configs.remove("system.communication.useSignatures");
			if (s == null) {
				useSignatures = 0;
			} else {
				useSignatures = Integer.parseInt(s);
			}

			s = (String) configs.remove("system.totalordermulticast.state_transfer");
			if (s == null) {
				stateTransferEnabled = false;
			} else {
				stateTransferEnabled = Boolean.parseBoolean(s);
			}

			s = (String) configs.remove("system.totalordermulticast.checkpoint_period");
			if (s == null) {
				checkpointPeriod = 1;
			} else {
				checkpointPeriod = Integer.parseInt(s);
			}

			s = (String) configs.remove("system.communication.useControlFlow");
			if (s == null) {
				useControlFlow = 0;
			} else {
				useControlFlow = Integer.parseInt(s);
			}

			s = (String) configs.remove("system.initial.view");
			if (s == null) {
				initialView = new int[n];
				for (int i = 0; i < n; i++) {
					initialView[i] = i;
				}
			}
			// bftsmart origin code
			else {
				StringTokenizer str = new StringTokenizer(s, ",");
				initialView = new int[str.countTokens()];
				for (int i = 0; i < initialView.length; i++) {
					initialView[i] = Integer.parseInt(str.nextToken());
				}
			}

			s = (String) configs.remove("system.ttp.id");
			if (s == null) {
				ttpId = -1;
			} else {
				ttpId = Integer.parseInt(s);
			}

			s = (String) configs.remove("system.communication.inQueueSize");
			if (s == null) {
				inQueueSize = 1000;
			} else {

				inQueueSize = Integer.parseInt(s);
				if (inQueueSize < 1) {
					inQueueSize = 1000;
				}

			}

			s = (String) configs.remove("system.communication.outQueueSize");
			if (s == null) {
				outQueueSize = 1000;
			} else {
				outQueueSize = Integer.parseInt(s);
				if (outQueueSize < 1) {
					outQueueSize = 1000;
				}
			}

			s = (String) configs.remove("system.totalordermulticast.log");
			if (s != null) {
				isToLog = Boolean.parseBoolean(s);
			} else {
				isToLog = false;
			}

			s = (String) configs.remove("system.totalordermulticast.log_parallel");
			if (s != null) {
				parallelLog = Boolean.parseBoolean(s);
			} else {
				parallelLog = false;
			}

			s = (String) configs.remove("system.totalordermulticast.log_to_disk");
			if (s != null) {
				logToDisk = Boolean.parseBoolean(s);
			} else {
				logToDisk = false;
			}

			s = (String) configs.remove("system.totalordermulticast.sync_log");
			if (s != null) {
				syncLog = Boolean.parseBoolean(s);
			} else {
				syncLog = false;
			}

			s = (String) configs.remove("system.totalordermulticast.checkpoint_to_disk");
			if (s == null) {
				isToWriteCkpsToDisk = false;
			} else {
				isToWriteCkpsToDisk = Boolean.parseBoolean(s);
			}

			s = (String) configs.remove("system.totalordermulticast.sync_ckp");
			if (s == null) {
				syncCkp = false;
			} else {
				syncCkp = Boolean.parseBoolean(s);
			}

			s = (String) configs.remove("system.totalordermulticast.global_checkpoint_period");
			if (s == null) {
				globalCheckpointPeriod = 1;
			} else {
				globalCheckpointPeriod = Integer.parseInt(s);
			}

			s = (String) configs.remove("system.bft");
			isBFT = (s != null) ? Boolean.parseBoolean(s) : true;

			s = (String) configs.remove("system.numrepliers");
			if (s == null) {
				numRepliers = 0;
			} else {
				numRepliers = Integer.parseInt(s);
			}

			s = (String) configs.remove("system.numnettyworkers");
			if (s == null) {
				numNettyWorkers = 0;
			} else {
				numNettyWorkers = Integer.parseInt(s);
			}
		} catch (Exception e) {
			e.printStackTrace(System.err);
		}

	}

	@Override
	public Properties getConfigProperties() {
		Properties configs = new Properties();

		configs.setProperty("system.autoconnect", autoConnectLimit + "");

		configs.setProperty("system.channels.blocking", Boolean.toString(channelsBlocking));

		configs.setProperty("system.communication.defaultkeys", Boolean.toString(defaultKeys));

		configs.setProperty("system.diffie-hellman.p", DH_P.toString(16));

		configs.setProperty("system.diffie-hellman.g", DH_G.toString());

		return configs;
	}

	@Override
	public boolean isUseDefaultKeys() {
		return defaultKeys;
	}

	@Override
	public boolean isHostSetted(int id) {
		if (hostsConfig.getHost(id) == null) {
			return false;
		}
		return true;
	}

	@Override
	public boolean isUseBlockingChannels() {
		return this.channelsBlocking;
	}

	@Override
	public int getAutoConnectLimit() {
		return this.autoConnectLimit;
	}

	@Override
	public BigInteger getDHP() {
		return DH_P;
	}

	@Override
	public BigInteger getDHG() {
		return DH_G;
	}

	@Override
	public String getHmacAlgorithm() {
		return hmacAlgorithm;
	}

	@Override
	public int getHmacSize() {
		return hmacSize;
	}

	@Override
	public NodeNetwork getRemoteAddress(int id) {
		if (hostsConfig.getRemoteAddress(id) != null) {
			return hostsConfig.getRemoteAddress(id);
		} else {
			return new NullNodeNetwork();
		}
	}

	@Override
	public NodeNetwork getServerToServerRemoteAddress(int id) {
		return hostsConfig.getServerToServerRemoteAddress(id);
	}

	@Override
	public NodeNetwork getLocalAddress(int id) {
		return hostsConfig.getLocalAddress(id);
	}

	@Override
	public String getHost(int id) {
		return hostsConfig.getHost(id);
	}

	@Override
	public int getPort(int id) {
		return hostsConfig.getPort(id);
	}

	@Override
	public int getMonitorPort(int id) {
		return hostsConfig.getMonitorPort(id);
	}

	@Override
	public int getServerToServerPort(int id) {
		return hostsConfig.getServerToServerPort(id);
	}

	@Override
	public int getProcessId() {
		return processId;
	}

	@Override
	public String getViewStoreClass() {
		String s = (String) systemConfig.remove("view.storage.handler");
		if (s == null) {
			return "bftsmart.reconfiguration.views.DefaultViewStorage";
		} else {
			return s;
		}

	}

//	@Override
//	public boolean isTheTTP() {
//		return (this.getTTPId() == this.getProcessId());
//	}

	@Override
	public final int[] getInitialView() {
		return this.initialView;
	}

//	@Override
//	public int getTTPId() {
//		return ttpId;
//	}

	@Override
	public int getRequestTimeout() {
		return requestTimeout;
	}

	@Override
	public int getClientDatasMonitorTimeout() {
		return clientDatasMonitorTimeout;
	}

	@Override
	public int getClientDatasMaxCount() {
		return clientDatasMaxCount;
	}

	@Override
	public long getHeartBeatTimeout() {
		return heartBeatTimeout;
	}

	@Override
	public long getHeartBeatPeriod() {
		return heartBeatPeriod;
	}

	@Override
	public long getTimeTolerance() {
		return timeTolerance;
	}

	@Override
	public int getReplyVerificationTime() {
		return replyVerificationTime;
	}

	@Override
	public int getN() {
		return n;
	}

	@Override
	public int getF() {
		return f;
	}

	@Override
	public int getPaxosHighMark() {
		return paxosHighMark;
	}

	@Override
	public int getRevivalHighMark() {
		return revivalHighMark;
	}

	@Override
	public int getTimeoutHighMark() {
		return timeoutHighMark;
	}

	@Override
	public int getMaxBatchSize() {
		return maxBatchSize;
	}

	@Override
	public boolean isShutdownHookEnabled() {
		return shutdownHookEnabled;
	}

	@Override
	public boolean isStateTransferEnabled() {
		return stateTransferEnabled;
	}

	@Override
	public int getInQueueSize() {
		return inQueueSize;
	}

	@Override
	public int getOutQueueSize() {
		return outQueueSize;
	}

//	public boolean isUseSenderThread() {
//		return useSenderThread;
//	}

	/**
	 * 消息发送失败的重试间隔；单位为“毫秒”；
	 * 
	 * @return
	 */
	@Override
	public long getSendRetryInterval() {
		return sendRetryInterval;
	}

	/**
	 * 消息发送失败的重试次数；
	 * 
	 * @return
	 */
	@Override
	public int getSendRetryCount() {
		return sendRetryCount;
	}

	/**
	 * *
	 */
	@Override
	public int getNumberOfNIOThreads() {
		return numNIOThreads;
	}

	/** * @return the numberOfNonces */
	@Override
	public int getNumberOfNonces() {
		return numberOfNonces;
	}

	/**
	 * Indicates if signatures should be used (1) or not (0) to authenticate client
	 * requests
	 */
	@Override
	public int getUseSignatures() {
		return useSignatures;
	}

	/**
	 * Indicates if MACs should be used (1) or not (0) to authenticate client-server
	 * and server-server messages
	 */
	@Override
	public int getUseMACs() {
		return useMACs;
	}

	/**
	 * Indicates the checkpoint period used when fetching the state from the
	 * application
	 */
	@Override
	public int getCheckpointPeriod() {
		return checkpointPeriod;
	}

	@Override
	public boolean isToWriteCkpsToDisk() {
		return isToWriteCkpsToDisk;
	}

	@Override
	public boolean isToWriteSyncCkp() {
		return syncCkp;
	}

	@Override
	public boolean isToLog() {
		return isToLog;
	}

	@Override
	public boolean isToWriteSyncLog() {
		return syncLog;
	}

	@Override
	public boolean isLoggingToDisk() {
		return logToDisk;
	}

	@Override
	public boolean isToLogParallel() {
		// TODO Auto-generated method stub
		return parallelLog;
	}

	/**
	 * Indicates the checkpoint period used when fetching the state from the
	 * application
	 */
	@Override
	public int getGlobalCheckpointPeriod() {
		return globalCheckpointPeriod;
	}

	/**
	 * Indicates if a simple control flow mechanism should be used to avoid an
	 * overflow of client requests
	 */
	@Override
	public int getUseControlFlow() {
		return useControlFlow;
	}

	// public PublicKey getRSAPublicKey() {
	// try {
	// return rsaLoader.loadPublicKey();
	// } catch (Exception e) {
	// e.printStackTrace(System.err);
	// return null;
	// }
	//
	// }

	/**
	 * Get RSAPublicKey of the specified process;
	 * 
	 * @param id the id of process;
	 * @return
	 */
	@Override
	public PublicKey getRSAPublicKey(int id) {
		try {
			return rsaLoader.loadPublicKey(id);
		} catch (Exception e) {
			e.printStackTrace(System.err);
			return null;
		}

	}

	/**
	 * Get RSAPrivateKey of current process;
	 * 
	 * @return
	 */
	@Override
	public PrivateKey getRSAPrivateKey() {
		try {
			return rsaLoader.loadPrivateKey(this.processId);
		} catch (Exception e) {
			e.printStackTrace(System.err);
			return null;
		}
	}

	@Override
	public boolean isBFT() {

		return this.isBFT;
	}

	@Override
	public int getNumRepliers() {
		return numRepliers;
	}

	@Override
	public int getNumNettyWorkers() {
		return numNettyWorkers;
	}

	@Override
	public HostsConfig getOuterHostConfig() {
		return outerHostConfig;
	}

	public void updateConfiguration(int[] newView, int n, int f) {
		this.initialView = newView;
		this.n = n;
		this.f = f;
	}

	public void setProcessId(int processId) {
		this.processId = processId;
	}

	@Override
	public void addHostInfo(int id, String host, int port, int monitorPort) {
		this.hostsConfig.add(id, host, port, monitorPort);
	}

}
