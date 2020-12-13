package bftsmart.tom.leaderchange;

import bftsmart.tom.core.TOMLayer;
import org.apache.commons.collections4.map.LRUMap;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This thread serves as a manager for all timers of pending requests.
 *
 */
public class HeartBeatTimer {

	// 重复发送LeaderRequest的间隔时间
	private static final long RESEND_MILL_SECONDS = 10000;

	private static final long DELAY_MILL_SECONDS = 30000;

	private static final long LEADER_DELAY_MILL_SECONDS = 20000;

	private static final long LEADER_STATUS_MILL_SECONDS = 60000;

	private static final long LEADER_STATUS_MAX_WAIT = 5000;

	private static final long LEADER_REQUEST_MILL_SECONDS = 60000L;

	private static final long STOP_WAIT_SECONDS = 30L;

	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(HeartBeatTimer.class);

	private final Map<Long, List<LeaderResponseMessage>> leaderResponseMap = new LRUMap<>(1024 * 8);

	private ScheduledExecutorService leaderTimer = Executors.newSingleThreadScheduledExecutor();

	private ScheduledExecutorService replicaTimer = Executors.newSingleThreadScheduledExecutor();

	private ScheduledExecutorService leaderResponseTimer = Executors.newSingleThreadScheduledExecutor();

	private ScheduledExecutorService leaderChangeStartThread = Executors.newSingleThreadScheduledExecutor();

	private ExecutorService stopThreadExecutor = Executors.newSingleThreadExecutor();

	private TOMLayer tomLayer; // TOM layer

	private volatile boolean isSendLeaderRequest = false;

	private volatile long lastSendLeaderRequestTime = -1L;

	private volatile InnerHeartBeatMessage innerHeartBeatMessage;

	private volatile long lastLeaderRequestSequence = -1L;

	private volatile long lastLeaderStatusSequence = -1L;

	private volatile LeaderStatusContext leaderStatusContext = new LeaderStatusContext(-1L);

//    private volatile boolean isLeaderChangeRunning = false;

	private Lock lrLock = new ReentrantLock();

	private Lock hbLock = new ReentrantLock();

	private Lock lsLock = new ReentrantLock();

	public HeartBeatTimer() {

	}

	public void start() {
		leaderTimerStart();
		replicaTimerStart();
	}

	public void restart() {
		stopAll();
		start();
	}

//    public void startLeaderChange() {
//        this.isLeaderChangeRunning = true;
//    }
//
//    public void stopLeaderChange() {
//        stopThreadExecutor.execute(() -> {
//            try {
//                // 延时30s再停止，便于新的Leader发送心跳
//                TimeUnit.SECONDS.sleep(STOP_WAIT_SECONDS);
//                this.isLeaderChangeRunning = false;
//            } catch (Exception e) {
//                LOGGER.error("stop lc change !", e);
//            }
//        });
//    }

	public void leaderTimerStart() {
		// stop Replica timer，and start leader timer
		if (leaderTimer == null) {
			leaderTimer = Executors.newSingleThreadScheduledExecutor();
		}
		leaderTimer.scheduleWithFixedDelay(new LeaderTimerTask(), LEADER_DELAY_MILL_SECONDS,
				tomLayer.controller.getStaticConf().getHeartBeatPeriod(), TimeUnit.MILLISECONDS);
	}

	public void replicaTimerStart() {
		if (replicaTimer == null) {
			replicaTimer = Executors.newSingleThreadScheduledExecutor();
		}
		replicaTimer.scheduleWithFixedDelay(new ReplicaTimerTask(), DELAY_MILL_SECONDS,
				tomLayer.controller.getStaticConf().getHeartBeatTimeout(), TimeUnit.MILLISECONDS);
	}

	public void stopAll() {
		if (replicaTimer != null) {
			replicaTimer.shutdownNow();
		}
		if (leaderTimer != null) {
			leaderTimer.shutdownNow();
		}
		replicaTimer = null;
		leaderTimer = null;
	}

	public void shutdown() {
		stopAll();
	}

	/**
	 * 收到心跳消息
	 * 
	 * @param heartBeatMessage
	 */
	public void receiveHeartBeatMessage(HeartBeatMessage heartBeatMessage) {
		hbLock.lock();
		try {
			// 需要考虑是否每次都更新innerHeartBeatMessage
			if (heartBeatMessage.getLeader() == tomLayer.leader()) {
				innerHeartBeatMessage = new InnerHeartBeatMessage(System.currentTimeMillis(), heartBeatMessage);
				if (heartBeatMessage.getLastRegency() != tomLayer.getSynchronizer().getLCManager().getLastReg()) {
					sendLeaderRequestMessage();
				}
			} else {
				sendLeaderRequestMessage();
			}
		} finally {
			hbLock.unlock();
		}
	}

	/**
	 * 收到其他节点发送来的领导者状态请求消息
	 *
	 * @param requestMessage
	 */
	public void receiveLeaderStatusRequestMessage(LeaderStatusRequestMessage requestMessage) {
		hbLock.lock();
		try {
			LOGGER.info("I am {}, receive leader status request for [{}:{}] !",
					tomLayer.controller.getStaticConf().getProcessId(), requestMessage.getSender(),
					requestMessage.getSequence());
			LeaderStatusResponseMessage responseMessage = new LeaderStatusResponseMessage(
					tomLayer.controller.getStaticConf().getProcessId(), requestMessage.getSequence(), tomLayer.leader(),
					tomLayer.getSynchronizer().getLCManager().getLastReg(), checkLeaderStatus());
			// 判断当前本地节点的状态
			// 首先判断leader是否一致
//            if (tomLayer.leader() != requestMessage.getLeaderId()) {
//                // leader不一致，则返回不一致的情况
//                responseMessage.setStatus(LeaderStatusResponseMessage.LEADER_STATUS_NOTEQUAL);
//            } else {
//                // 判断时间戳的一致性
//                responseMessage.setStatus(checkLeaderStatus());
//            }
			LOGGER.info(
					"I am {}, send leader status response [sequence: leader: regency: timeoutStatus]-[{}:{}:{}:{}] to {} !",
					tomLayer.controller.getStaticConf().getProcessId(), responseMessage.getSequence(),
					responseMessage.getLeaderId(), responseMessage.getRegency(), responseMessage.getStatus(),
					requestMessage.getSender());
			// 将该消息发送给请求节点
			int[] to = new int[1];
			to[0] = requestMessage.getSender();
			tomLayer.getCommunication().send(to, responseMessage);
		} finally {
			hbLock.unlock();
		}
	}

	/**
	 * 处理收到的应答消息
	 *
	 * @param responseMessage
	 */
	public void receiveLeaderStatusResponseMessage(LeaderStatusResponseMessage responseMessage) {
		LOGGER.info(
				"I am {}, receive leader status response from {}, which status = [sequence: leader: regency: timeoutStatus]-[{}:{}:{}:{}] !",
				tomLayer.controller.getStaticConf().getProcessId(), responseMessage.getSender(),
				responseMessage.getSequence(), responseMessage.getLeaderId(), responseMessage.getRegency(),
				responseMessage.getStatus());
		lsLock.lock();
		try {
			long sequence = leaderStatusContext.getSequence();
			// 将状态加入到其中
			LeaderStatus leaderStatus = leaderStatus(responseMessage);
			leaderStatusContext.addStatus(responseMessage.getSender(), leaderStatus, sequence);
		} finally {
			lsLock.unlock();
		}
	}

	/**
	 * 领导者状态
	 *
	 * @param responseMessage
	 * @return
	 */
	private LeaderStatus leaderStatus(LeaderStatusResponseMessage responseMessage) {
		return new LeaderStatus(responseMessage.getStatus(), responseMessage.getLeaderId(),
				responseMessage.getRegency());
	}

	/**
	 * 检查本地节点领导者状态
	 *
	 *
	 * @return
	 */
	private int checkLeaderStatus() {
		if (tomLayer.leader() == tomLayer.controller.getStaticConf().getProcessId()) {
			// 如果我是Leader，返回正常
			return LeaderStatusResponseMessage.LEADER_STATUS_NORMAL;
		}
		// 需要判断所有连接是否已经成功建立
		if (!tomLayer.isConnectRemotesOK()) {
			// 流程还没有处理完的话，也返回成功
			return LeaderStatusResponseMessage.LEADER_STATUS_NORMAL;
		}
		if (innerHeartBeatMessage == null) {
			// 有超时，则返回超时
			if (tomLayer.requestsTimer != null) {
				return LeaderStatusResponseMessage.LEADER_STATUS_TIMEOUT;
			}
		} else {
			// 判断时间
			long lastTime = innerHeartBeatMessage.getTime();
			if (System.currentTimeMillis() - lastTime > tomLayer.controller.getStaticConf().getHeartBeatTimeout()) {
				// 此处触发超时
				if (tomLayer.requestsTimer != null) {
					return LeaderStatusResponseMessage.LEADER_STATUS_TIMEOUT;
				}
			}
		}
		return LeaderStatusResponseMessage.LEADER_STATUS_NORMAL;
	}

	/**
	 * 收到领导者请求
	 * 
	 * @param requestMessage
	 */
	public void receiveLeaderRequestMessage(LeaderRequestMessage requestMessage) {
		// 获取当前节点的领导者信息，然后应答给发送者
		int currLeader = tomLayer.leader();
		LeaderResponseMessage responseMessage = new LeaderResponseMessage(
				tomLayer.controller.getStaticConf().getProcessId(), currLeader, requestMessage.getSequence(),
				tomLayer.getSynchronizer().getLCManager().getLastReg());
		int[] to = new int[1];
		to[0] = requestMessage.getSender();
		tomLayer.getCommunication().send(to, responseMessage);
	}

	/**
	 * 收到领导者应答请求
	 * 
	 * @param responseMessage
	 */
	public void receiveLeaderResponseMessage(LeaderResponseMessage responseMessage) {
		// 判断是否是自己发送的sequence
		lrLock.lock();
		try {
			long msgSeq = responseMessage.getSequence();
			if (msgSeq == lastLeaderRequestSequence) {
				// 是当前节点发送的请求，则将其加入到Map中
				List<LeaderResponseMessage> responseMessages = leaderResponseMap.get(msgSeq);
				if (responseMessages == null) {
					responseMessages = new ArrayList<>();
					responseMessages.add(responseMessage);
					leaderResponseMap.put(msgSeq, responseMessages);
				} else {
					responseMessages.add(responseMessage);
				}
				// 判断收到的心跳信息是否满足
				NewLeader newLeader = newLeader(responseMessage.getSequence());
				if (newLeader != null) {
					if (leaderResponseTimer != null) {
						leaderResponseTimer.shutdownNow(); // 取消定时器
						leaderResponseTimer = null;
					}
					// 表示满足条件，设置新的Leader与regency
					// 如果我本身是领导者，又收到来自其他领导者的心跳，经过领导者查询之后需要取消一个领导者定时器
					if ((tomLayer.leader() != newLeader.getNewLeader())
							|| (tomLayer.getSynchronizer().getLCManager().getLastReg() != newLeader.getLastRegency())) {
						// 重置leader和regency
						tomLayer.execManager.setNewLeader(newLeader.getNewLeader()); // 设置新的Leader
						tomLayer.getSynchronizer().getLCManager().setNewLeader(newLeader.getNewLeader()); // 设置新的Leader
						tomLayer.getSynchronizer().getLCManager().setNextReg(newLeader.getLastRegency());
						tomLayer.getSynchronizer().getLCManager().setLastReg(newLeader.getLastRegency());
						// 重启定时器
						restart();
					}
					// 重置last regency以后，所有在此之前添加的stop 重传定时器需要取消
					tomLayer.getSynchronizer()
							.removeSTOPretransmissions(tomLayer.getSynchronizer().getLCManager().getLastReg());
					isSendLeaderRequest = false;
				}
			} else {
				// 收到的心跳信息有问题，打印日志
				LOGGER.error("I am proc {} , receive leader response from {}, last sequence {}, receive sequence {}",
						tomLayer.controller.getStaticConf().getProcessId(), responseMessage.getSender(),
						lastLeaderRequestSequence, msgSeq);
			}
		} finally {
			lrLock.unlock();
		}
	}

	public void setTomLayer(TOMLayer tomLayer) {
		this.tomLayer = tomLayer;
	}

	public void sendLeaderRequestMessage() {
		// 假设收到的消息不是当前的Leader，则需要发送获取其他节点Leader
		long sequence = System.currentTimeMillis();
		lrLock.lock();
		try {
			// 防止一段时间内重复发送多次心跳请求
			if (!isSendLeaderRequest || (sequence - lastSendLeaderRequestTime) > LEADER_REQUEST_MILL_SECONDS) {
				lastSendLeaderRequestTime = sequence;
				sendLeaderRequestMessage(sequence);
			}
		} finally {
			lrLock.unlock();
		}
	}

	private void sendLeaderRequestMessage(long sequence) {
		LeaderRequestMessage requestMessage = new LeaderRequestMessage(
				tomLayer.controller.getStaticConf().getProcessId(), sequence);
		tomLayer.getCommunication().send(tomLayer.controller.getCurrentViewOtherAcceptors(), requestMessage);
		lastLeaderRequestSequence = sequence;
		isSendLeaderRequest = true;
		// 启动定时任务，判断心跳的应答处理
		if (leaderResponseTimer == null) {
			leaderResponseTimer = Executors.newSingleThreadScheduledExecutor();
			leaderResponseTimer.scheduleWithFixedDelay(new LeaderResponseTask(lastLeaderRequestSequence), 0,
					RESEND_MILL_SECONDS, TimeUnit.MILLISECONDS);
		} else {
			leaderResponseTimer.shutdownNow();
			if (leaderResponseTimer.isShutdown()) {
				// 重新开启新的任务
				leaderResponseTimer = Executors.newSingleThreadScheduledExecutor();
				leaderResponseTimer.scheduleWithFixedDelay(new LeaderResponseTask(lastLeaderRequestSequence), 0,
						RESEND_MILL_SECONDS, TimeUnit.MILLISECONDS);
			}
		}
	}

	/**
	 * 获取新的Leader
	 * 
	 * @return 返回null表示未达成一致，否则表示达成一致
	 */
	private NewLeader newLeader(long currentSequence) {
		// 从缓存中获取应答
		List<LeaderResponseMessage> leaderResponseMessages = leaderResponseMap.get(currentSequence);
		if (leaderResponseMessages == null || leaderResponseMessages.isEmpty()) {
			return null;
		} else {
			return newLeaderCheck(leaderResponseMessages, tomLayer.controller.getCurrentViewF());
		}
	}

	/**
	 * 新领导者check过程
	 * 
	 * @param leaderResponseMessages
	 * @return
	 */
	public static NewLeader newLeaderCheck(List<LeaderResponseMessage> leaderResponseMessages, int f) {
		// 判断收到的应答结果是不是满足2f+1的规则
		Map<Integer, Integer> leader2Size = new HashMap<>();
		Map<Integer, Integer> regency2Size = new HashMap<>();
		// 防止重复
		Set<Integer> nodeSet = new HashSet<>();
		for (LeaderResponseMessage lrm : leaderResponseMessages) {
			int currentLeader = lrm.getLeader();
			int currentRegency = lrm.getLastRegency();
			int currentNode = lrm.getSender();
			if (!nodeSet.contains(currentNode)) {
				leader2Size.merge(currentLeader, 1, Integer::sum);
				regency2Size.merge(currentRegency, 1, Integer::sum);
				nodeSet.add(currentNode);
			}
		}
		// 获取leaderSize最大的Leader
		int leaderMaxSize = -1;
		int leaderMaxId = -1;
		for (Map.Entry<Integer, Integer> entry : leader2Size.entrySet()) {
			int currLeaderId = entry.getKey(), currLeaderSize = entry.getValue();
			if (currLeaderSize > leaderMaxSize) {
				leaderMaxId = currLeaderId;
				leaderMaxSize = currLeaderSize;
			}
		}

		// 获取leaderSize最大的Leader
		int regencyMaxSize = -1;
		int regencyMaxId = -1;
		for (Map.Entry<Integer, Integer> entry : regency2Size.entrySet()) {
			int currRegency = entry.getKey(), currRegencySize = entry.getValue();
			if (currRegencySize > regencyMaxSize) {
				regencyMaxId = currRegency;
				regencyMaxSize = currRegencySize;
			}
		}

		// 判断是否满足2f+1
		int compareLeaderSize = 2 * f + 1;
		int compareRegencySize = 2 * f + 1;

		if (leaderMaxSize >= compareLeaderSize && regencyMaxSize >= compareRegencySize) {
			return new NewLeader(leaderMaxId, regencyMaxId);
		}
		return null;
	}

	/**
	 *
	 */
	class LeaderTimerTask implements Runnable {

		@Override
		public void run() {
			try {
				// 再次判断是否是Leader
				if (tomLayer.isLeader()) {
					if (!tomLayer.isConnectRemotesOK()) {
						return;
					}
					// 如果是Leader则发送心跳信息给其他节点，当前节点除外
					HeartBeatMessage heartBeatMessage = new HeartBeatMessage(
							tomLayer.controller.getStaticConf().getProcessId(),
							tomLayer.controller.getStaticConf().getProcessId(),
							tomLayer.getSynchronizer().getLCManager().getLastReg());
					tomLayer.getCommunication().send(tomLayer.controller.getCurrentViewOtherAcceptors(),
							heartBeatMessage);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	class LeaderResponseTask implements Runnable {

		private final long currentSequence;

		LeaderResponseTask(long currentSequence) {
			this.currentSequence = currentSequence;
		}

		@Override
		public void run() {
			lrLock.lock();
			try {
				NewLeader newLeader = newLeader(currentSequence);
				if (newLeader != null) {
					// 满足，则更新当前节点的Leader
					tomLayer.execManager.setNewLeader(newLeader.getNewLeader());
					tomLayer.getSynchronizer().getLCManager().setNextReg(newLeader.getLastRegency());
					tomLayer.getSynchronizer().getLCManager().setLastReg(newLeader.getLastRegency());
					leaderResponseTimer.shutdownNow();
					leaderResponseTimer = null;
					isSendLeaderRequest = false;
				}
			} finally {
				lrLock.unlock();
			}
		}
	}

	class ReplicaTimerTask extends TimerTask {

		@Override
		public void run() {
			// 再次判断是否是Leader
			if (!tomLayer.isLeader()) {
				// 检查收到的InnerHeartBeatMessage是否超时
				hbLock.lock();
				try {
					// 需要判断所有连接是否已经成功建立
					if (!tomLayer.isConnectRemotesOK()) {
						return;
					}
					if (innerHeartBeatMessage == null) {
						// 此处触发超时
						LOGGER.info("I am proc {} trigger hb timeout, because heart beat message is NULL !!!",
								tomLayer.controller.getStaticConf().getProcessId());
						if (tomLayer.requestsTimer != null) {
							leaderChangeStartThread.execute(new LeaderChangeTask());
						}
					} else {
						// 判断时间
						long lastTime = innerHeartBeatMessage.getTime();
						if (System.currentTimeMillis() - lastTime > tomLayer.controller.getStaticConf()
								.getHeartBeatTimeout()) {
							// 此处触发超时
							LOGGER.info("I am proc {} trigger hb timeout, time = {}, last hb time = {}",
									tomLayer.controller.getStaticConf().getProcessId(), System.currentTimeMillis(),
									innerHeartBeatMessage.getTime());
							if (tomLayer.requestsTimer != null) {
								leaderChangeStartThread.execute(new LeaderChangeTask());
							}
						}
					}
				} finally {
					hbLock.unlock();
				}
			}
		}
	}

	class LeaderChangeTask implements Runnable {

		@Override
		public void run() {
			// 检查是否确实需要进行领导者切换
			// 首先发送其他节点领导者切换的消息，然后等待
			long currentTimeMillis = System.currentTimeMillis();
			if (currentTimeMillis - lastLeaderStatusSequence > LEADER_STATUS_MILL_SECONDS) {
				// 重置sequence
				lastLeaderStatusSequence = currentTimeMillis;
				// 可以开始处理
				// 首先生成领导者状态请求消息
				LeaderStatusRequestMessage requestMessage = new LeaderStatusRequestMessage(
						tomLayer.controller.getStaticConf().getProcessId(), currentTimeMillis);
				LOGGER.info("I am {}, send leader status request[{}] to others !", requestMessage.getSender(),
						requestMessage.getSequence());
				lsLock.lock();
				try {
					// 通过锁进行控制
					leaderStatusContext = new LeaderStatusContext(currentTimeMillis);
					// 调用发送线程
					tomLayer.getCommunication().send(tomLayer.controller.getCurrentViewOtherAcceptors(),
							requestMessage);
				} finally {
					lsLock.unlock();
				}
				try {
					// 等待应答结果
//					Map<Integer, LeaderStatus> statusMap = leaderStatusContext
//							.waitLeaderStatuses(LEADER_STATUS_MAX_WAIT);
					// 状态不为空的情况下进行汇总，并且是同一个Sequence
					if (leaderStatusContext.waitLeaderStatuses(LEADER_STATUS_MAX_WAIT)) {
						LOGGER.info("I am {}, receive more than f response for timeout, so I will start to LC !",
								tomLayer.controller.getStaticConf().getProcessId());
						// 再次进行判断，防止LC已处理完成再次触发
//                            if (!isLeaderChangeRunning) {
//                                startLeaderChange();
						// 表示可以触发领导者切换流程

						// 对于不一致的leaderid, regency进行本地更新，达到节点之间的一致性
//						checkAndUpdateLeaderInfos(statusMap);
						GlobalRegencyInfo globalRegencyInfo = leaderStatusContext.getGlobalRegencyInfo();

						tomLayer.requestsTimer.run_lc_protocol(globalRegencyInfo);
//                            }
					}
				} catch (Exception e) {
					LOGGER.info("Handle leader status response error !!!", e);
				}
			} // End of: if (currentTimeMillis - lastLeaderStatusSequence >
				// LEADER_STATUS_MILL_SECONDS)
		}
	}

	class InnerHeartBeatMessage {

		private long time;

		private HeartBeatMessage heartBeatMessage;

		public InnerHeartBeatMessage(long time, HeartBeatMessage heartBeatMessage) {
			this.time = time;
			this.heartBeatMessage = heartBeatMessage;
		}

		public long getTime() {
			return time;
		}

		public HeartBeatMessage getHeartBeatMessage() {
			return heartBeatMessage;
		}
	}

	static class NewLeader {

		private int newLeader;

		private int lastRegency;

		public NewLeader(int newLeader, int lastRegency) {
			this.newLeader = newLeader;
			this.lastRegency = lastRegency;
		}

		public int getNewLeader() {
			return newLeader;
		}

		public int getLastRegency() {
			return lastRegency;
		}
	}

	class LeaderStatus {

		private int status;

		private int leader;

		private int regency;

		public LeaderStatus(int status, int leader, int regency) {
			this.status = status;
			this.leader = leader;
			this.regency = regency;
		}

		public int getStatus() {
			return status;
		}

		public int getLeader() {
			return leader;
		}

		public int getRegency() {
			return regency;
		}
	}

	class LeaderStatusContext {

		private long sequence;

		private CountDownLatch latch = new CountDownLatch(1);

		private Map<Integer, LeaderStatus> leaderStatuses = new ConcurrentHashMap<>();

		public LeaderStatusContext(long sequence) {
			this.sequence = sequence;
		}

		public synchronized void addStatus(int sender, LeaderStatus leaderStatus, long sequence) {
			if (this.sequence == sequence) {
				// 将状态加入到其中
				leaderStatuses.put(sender, leaderStatus);

				if (isStatusTimeout()) {
					latch.countDown();
				}

			} else {
				LOGGER.warn("I am {}, receive leader status response[{}:{}] is not match !!!",
						tomLayer.controller.getStaticConf().getProcessId(), sender, sequence);
			}
		}

//		private void reset() {
//			sequence = -1L;
//			leaderStatuses.clear();
//			latch = null;
//		}
//
//		public synchronized void init(long sequence, int remoteSize) {
//			// 先进行重置
//			this.sequence = sequence;
//			
//		}

		public long getSequence() {
			return sequence;
		}

		/**
		 * 等待领导者状态查询结果的超时状态满足以下条件： <br>
		 * 1. 查询返回的所有节点中（？？也包括自身节点），处于领导者心跳超时的节点的数量满足：大于等于(n + f)/2 ;
		 * 
		 * <p>
		 * 方法会堵塞等待其它节点回复查询结果，最长等待指定的时长；
		 * <p>
		 * 
		 * 如果不满足数量条件，或者超时，都返回 false； 只有满足数量条件的情况下，才返回 true；
		 * 
		 * @param waitMillSeconds
		 * @return
		 * @throws Exception
		 */
		public boolean waitLeaderStatuses(long waitMillSeconds) throws Exception {
			latch.await(waitMillSeconds, TimeUnit.MILLISECONDS);
			return isStatusTimeout();
		}

		public synchronized boolean isStatusTimeout() {
			// 以（n + f）/ 2作为判断依据
			// 增强判断的条件，防止不必要的LC流程,如果因为网络差收不到足够的status响应，或者没有足够的共识节点心跳超时都不会触发LC流程
			// 注意：取值不能从this.tomLayer.controller.getStaticConf()中获得，激活新节点时TOMconfig并没有更新，只更新了视图
			int f = tomLayer.controller.getCurrentViewF();
			int n = tomLayer.controller.getCurrentViewN();
			int counter = (n + f) / 2;

			int receiveTimeoutSize = 0;
			for (Map.Entry<Integer, LeaderStatus> entry : leaderStatuses.entrySet()) {
				if (entry.getValue().getStatus() == LeaderStatusResponseMessage.LEADER_STATUS_TIMEOUT) {
					receiveTimeoutSize++;
				}
			}
			LOGGER.info("I am proc {}, receiveTimeoutSize = {}, counter = {}",
					tomLayer.controller.getStaticConf().getProcessId(), receiveTimeoutSize, counter);
			return receiveTimeoutSize >= counter;
		}

		public synchronized GlobalRegencyInfo getGlobalRegencyInfo() {
			int minLeader = tomLayer.getExecManager().getCurrentLeader();
			int maxRegency = tomLayer.getSynchronizer().getLCManager().getLastReg();
			boolean consistant = false;
			for (Map.Entry<Integer, LeaderStatus> entry : leaderStatuses.entrySet()) {
				int leader = entry.getValue().leader;
				int regency = entry.getValue().regency;

				if (leader != minLeader || regency != maxRegency) {
					consistant = true;
				}

				if (leader < minLeader) {
					minLeader = leader;
				}
				if (regency > maxRegency) {
					maxRegency = regency;
				}
			}

			return new GlobalRegencyInfo(minLeader, maxRegency, consistant);

//			if (maxRegency < tomLayer.getSynchronizer().getLCManager().getLastReg()) {
//				return;
//			}

			/// ???
//			if (tomLayer.getExecManager().getCurrentLeader() != minLeader) {
//				tomLayer.getExecManager().setNewLeader(minLeader);
//				tomLayer.getSynchronizer().getLCManager().setNewLeader(minLeader);
//			}
//			tomLayer.getSynchronizer().getLCManager().setNextReg(maxRegency);
//			tomLayer.getSynchronizer().getLCManager().setLastReg(maxRegency);
		}
	}

}
