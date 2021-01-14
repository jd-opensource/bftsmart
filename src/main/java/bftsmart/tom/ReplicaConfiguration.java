package bftsmart.tom;

import java.math.BigInteger;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Properties;

import bftsmart.reconfiguration.util.HostsConfig;
import bftsmart.reconfiguration.views.NodeNetwork;

public interface ReplicaConfiguration {

	int getProcessId();

	Properties getConfigProperties();

	boolean isUseDefaultKeys();

	boolean isHostSetted(int id);

	boolean isUseBlockingChannels();

	int getAutoConnectLimit();

	BigInteger getDHP();

	BigInteger getDHG();

	String getHmacAlgorithm();

	int getHmacSize();

	NodeNetwork getRemoteAddress(int id);

	NodeNetwork getServerToServerRemoteAddress(int id);

	NodeNetwork getLocalAddress(int id);

	String getHost(int id);

	int getPort(int id);

	int getMonitorPort(int id);

	int getServerToServerPort(int id);

	String getViewStoreClass();

	int[] getInitialView();
	
//	boolean isTheTTP();
//
//	int getTTPId();

	int getRequestTimeout();

	int getClientDatasMonitorTimeout();

	int getClientDatasMaxCount();

	long getHeartBeatTimeout();

	long getHeartBeatPeriod();

	long getTimeTolerance();

	int getReplyVerificationTime();

	int getN();

	int getF();

	int getPaxosHighMark();

	int getRevivalHighMark();

	int getTimeoutHighMark();

	int getMaxBatchSize();

	boolean isShutdownHookEnabled();

	boolean isStateTransferEnabled();

	int getInQueueSize();

	int getOutQueueSize();

	/**
	 * 消息发送失败的重试间隔；单位为“毫秒”；
	 * 
	 * @return
	 */
	long getSendRetryInterval();

	/**
	 * 消息发送失败的重试次数；
	 * 
	 * @return
	 */
	int getSendRetryCount();

	/**
	 * *
	 */
	int getNumberOfNIOThreads();

	/** * @return the numberOfNonces */
	int getNumberOfNonces();

	/**
	 * Indicates if signatures should be used (1) or not (0) to authenticate client
	 * requests
	 */
	int getUseSignatures();

	/**
	 * Indicates if MACs should be used (1) or not (0) to authenticate client-server
	 * and server-server messages
	 */
	int getUseMACs();

	/**
	 * Indicates the checkpoint period used when fetching the state from the
	 * application
	 */
	int getCheckpointPeriod();

	boolean isToWriteCkpsToDisk();

	boolean isToWriteSyncCkp();

	boolean isToLog();

	boolean isToWriteSyncLog();

	boolean isLoggingToDisk();

	boolean isToLogParallel();

	/**
	 * Indicates the checkpoint period used when fetching the state from the
	 * application
	 */
	int getGlobalCheckpointPeriod();

	/**
	 * Indicates if a simple control flow mechanism should be used to avoid an
	 * overflow of client requests
	 */
	int getUseControlFlow();

	/**
	 * Get RSAPublicKey of the specified process;
	 * 
	 * @param id the id of process;
	 * @return
	 */
	PublicKey getRSAPublicKey(int id);

	/**
	 * Get RSAPrivateKey of current process;
	 * 
	 * @return
	 */
	PrivateKey getRSAPrivateKey();

	boolean isBFT();

	int getNumRepliers();

	int getNumNettyWorkers();

	HostsConfig getOuterHostConfig();

	void addHostInfo(int id, String host, int port, int monitorPort);

}