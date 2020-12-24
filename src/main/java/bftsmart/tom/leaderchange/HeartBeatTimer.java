package bftsmart.tom.leaderchange;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.reconfiguration.views.View;
import bftsmart.tom.core.TOMLayer;

/**
 * This thread serves as a manager for all timers of pending requests.
 *
 */
public class HeartBeatTimer {

	private static final Logger LOGGER = LoggerFactory.getLogger(HeartBeatTimer.class);

	/**
	 * 启动初始化阶段的领导者确认任务的开始延迟；值为 10 秒；
	 */
	private static final long INIT_LEADER_CONFIRM_DELAY = 10000;
	/**
	 * 启动初始化阶段的领导者确认任务的超时时长；值为 30 秒；
	 */
	private static final long INIT_LEADER_CONFIRM_TIMEOUT = 30 * 1000;

	/**
	 * 正常阶段的领导者确认任务的开始延迟；值为 0 ，表示立即启动；
	 */
	private static final long NORMAL_LEADER_CONFIRM_DELAY = 0;
	/**
	 * 正常阶段的领导者确认任务的超时时长；值为 30 秒；
	 */
	private static final long NORMAL_LEADER_CONFIRM_TIMEOUT = 30 * 1000;

	private static final long NORMAL_DELAY = 2000;

	private ScheduledExecutorService leaderTimer;

	private ScheduledExecutorService followerTimer;

//	private ScheduledExecutorService leaderChangeStartThread = Executors.newSingleThreadScheduledExecutor();

	private TOMLayer tomLayer; // TOM layer

	private LeaderConfirmationTask leaderConfirmTask;

	/**
	 * TODO: 给予此变量的生命周期一个严格定义；
	 */
	private volatile HeartBeating heartBeatting;

	private volatile LeaderTimeoutTask leaderTimeoutTask;

	/**
	 * 是否已经完成初始化；
	 * <p>
	 * 
	 * 心跳定时器在创建后会先进行初始化，这个过程中会向其它节点轮询领导者的状态，并根据占据多数的结果来初始化自身的状态；
	 * <p>
	 * 在完成首次领导者轮询确认之前 {@link #initialized} 为 false ；
	 * <p>
	 * 在完成首次领导者轮询确认之后，无论结果怎样，{@link #initialized} 都设置为 true；
	 * <p>
	 * 领导者轮询确认的过程会关闭心跳定时器，并在完成之后重新开启心跳定时器；
	 */
	private boolean initialized;

	private boolean inactived;

	public HeartBeatTimer(TOMLayer tomLayer) {
		this.tomLayer = tomLayer;
		this.heartBeatting = new HeartBeating(new LeaderRegency(0, -1),
				tomLayer.controller.getStaticConf().getProcessId(), System.currentTimeMillis());

		// 启动后延迟 10 秒确认领导者；
		this.initialized = false;
		confirmLeaderRegency(INIT_LEADER_CONFIRM_DELAY, INIT_LEADER_CONFIRM_TIMEOUT);
	}

	private int getCurrentProcessId() {
		return tomLayer.controller.getStaticConf().getProcessId();
	}

	public synchronized void start() {
		leaderTimerStart(NORMAL_DELAY);
		followerTimerStart(NORMAL_DELAY);
	}

	public synchronized void restart() {
		stopAll();
		start();
	}

	public synchronized void stopAll() {
		innerStop();
	}

	/**
	 * 关闭；
	 */
	public synchronized void shutdown() {
		// 停止定时器；
		innerStop();

		// 发送 STOP 消息触发选举；
		triggerSTOP();
	}

	private void triggerSTOP() {
		try {
			if (tomLayer.isLeader()) {
				// 基于当前视图验证当前 Regency 的正确性;
				// 如果正确并且当前节点是领导者，则基于该执政期发送 STOP 消息，告知其它节点原领导者已经失效退出，需要重新选举；
				int currentProcessId = tomLayer.getCurrentProcessId();
				LeaderRegency currentRegency = tomLayer.getSynchronizer().getLCManager().getCurrentRegency();
				View currentView = tomLayer.controller.getCurrentView();
				int choosenLeader = LeaderRegencyPropose.chooseLeader(currentRegency.getId(), currentView);
				if (choosenLeader == currentRegency.getLeaderId() && choosenLeader == currentProcessId) {
					// 发送 STOP 消息，通知其它节点重新选举；
					LeaderRegencyPropose propose = LeaderRegencyPropose.chooseFromView(currentRegency.getId() + 1,
							currentView, currentProcessId);
					tomLayer.getSynchronizer().sendSTOP(propose);
					
					//等待 10 秒；
					Thread.sleep(10 *1000);
				}
			}
		} catch (Exception e) {
			LOGGER.warn("Error occurred when STOP was triggered before the shutdown of leader! --[LeaderId="
					+ tomLayer.getCurrentProcessId() + "] " + e.getMessage(), e);
		}
	}

	private void innerStop() {
		if (followerTimer != null) {
			followerTimer.shutdownNow();
		}
		if (leaderTimer != null) {
			leaderTimer.shutdownNow();
		}
		followerTimer = null;
		leaderTimer = null;
	}

	private long getHearBeatPeriod() {
		return tomLayer.controller.getStaticConf().getHeartBeatPeriod();
	}

	private void leaderTimerStart(long delay) {
		// stop Replica timer，and start leader timer
		if (initialized && leaderTimer == null) {
			leaderTimer = Executors.newSingleThreadScheduledExecutor();
			leaderTimer.scheduleWithFixedDelay(new LeaderHeartbeatBroadcastingTask(this), delay,
					tomLayer.controller.getStaticConf().getHeartBeatPeriod(), TimeUnit.MILLISECONDS);
		}
	}

	private void followerTimerStart(long delay) {
		if (initialized && followerTimer == null) {
			followerTimer = Executors.newSingleThreadScheduledExecutor();
			followerTimer.scheduleWithFixedDelay(new FollowerHeartbeatCheckingTask(), delay,
					tomLayer.controller.getStaticConf().getHeartBeatPeriod(), TimeUnit.MILLISECONDS);
		}
	}

	/**
	 * 收到心跳消息
	 * 
	 * @param heartBeatMessage
	 */
	public synchronized void receiveHeartBeatMessage(HeartBeatMessage heartBeatMessage) {
		// 需要考虑是否每次都更新innerHeartBeatMessage
		LeaderRegency beatingRegengy = new LeaderRegency(heartBeatMessage.getLeader(),
				heartBeatMessage.getLastRegency());
		int currentRegency = tomLayer.getSynchronizer().getLCManager().getLastReg();
		if (beatingRegengy.getLeaderId() == tomLayer.leader() && beatingRegengy.getId() == currentRegency) {
			// 领导者心跳正常；
			heartBeatting = new HeartBeating(beatingRegengy, heartBeatMessage.getSender(), System.currentTimeMillis());
		} else {
			// 收到的心跳执政期与当前节点所处的执政期不一致：要么执政期Id不相等，要么领导者不相等，要么两者都不等；
			// 1. 当心跳执政期大于当前节点所处的执政期，则向其它节点查询确认领导者执政期，并尝试同步到多数一致的状态；
			// 2. 当心跳执政期等于当前节点所处的执政期：
			// -- 2.1 如果当前节点是领导者，这意味着出现多个领导者，则停止本身的心跳，向其它节点查询确认领导者执政期，并尝试同步到多数一致的状态；
			// -- 2.2 如果当前节点不是领导者，这意味着此时心跳执政期的领导者和本地节点定义的领导者不同，则其它节点查询确认领导者执政期，并尝试同步到多数一致的状态；
			// 3. 当心跳执政期小于当前节点所处的执政期，则直接抛弃不必处理此心跳消息；
			// -- 由于此时当前节点所处的执政期的领导者节点也会发送心跳至这一过期的领导者节点，并促使该节点同步到最新的执政期；
			//
			// 总的来说，除了满足条件 3 之外，都应该向其它节点查询确认领导者执政期，并尝试同步到多数一致的状态；

			if (beatingRegengy.getId() < tomLayer.getSynchronizer().getLCManager().getLastReg()) {
				// 记录错误；
				LOGGER.warn(
						"Receive an outdated HeartBeatMessage with a smaller regency! --[CurrentRegency={}][BeatingRegency={}]",
						currentRegency, beatingRegengy);
			} else {
				// 向其它节点查询确认领导者执政期，并尝试同步到多数一致的状态；
				confirmLeaderRegency(NORMAL_LEADER_CONFIRM_DELAY, NORMAL_LEADER_CONFIRM_TIMEOUT);
			}
		}
	}

	/**
	 * 向其它节点查询确认领导者执政期，并尝试同步到多数一致的状态；
	 * <p>
	 * 此方法在收到领导者执政期不一致的心跳消息后出发，向网络中的其它节点询问“领导者执政期”；
	 * 
	 * @param startDelay 延迟启动的毫秒数；
	 */
	private synchronized void confirmLeaderRegency(long startDelay, long taskTimeout) {
		// 避免同时有多个领导者查询确认的任务在执行；
		if (leaderConfirmTask != null) {
			return;
		}

		leaderConfirmTask = new LeaderConfirmationTask(taskTimeout, this, this.tomLayer) {
			@Override
			protected void onCompleted() {
				leaderConfirmTask = null;
				initialized = true;
			}
		};

		leaderConfirmTask.start(startDelay);
	}

	/**
	 * 收到领导者请求
	 * 
	 * @param leaderRegencyRequest
	 */
	public void receiveLeaderRequestMessage(LeaderRequestMessage leaderRegencyRequest) {
		// 获取当前节点的领导者信息，然后应答给发送者
		LeaderRegency currentRegency = tomLayer.getSynchronizer().getLCManager().getCurrentRegency();
		LeaderResponseMessage leaderRegencyResponse = new LeaderResponseMessage(getCurrentProcessId(), currentRegency,
				this.tomLayer.controller.getCurrentView(), leaderRegencyRequest.getSequence());

		tomLayer.getCommunication().send(leaderRegencyResponse, leaderRegencyRequest.getSender());
	}

	/**
	 * 收到领导者应答请求
	 * 
	 * @param leaderRegencyResponse
	 */
	public synchronized void receiveLeaderResponseMessage(LeaderResponseMessage leaderRegencyResponse) {
		if (leaderConfirmTask != null) {
			leaderConfirmTask.receiveLeaderResponseMessage(leaderRegencyResponse);
		}
	}

	public void setLeaderInactived(boolean inactived) {
		this.inactived = inactived;
	}

	/**
	 * 心跳是否超时；
	 * 
	 * @return
	 */
	public boolean isHeartBeatTimeout() {
		return System.currentTimeMillis() - heartBeatting.getTime() > tomLayer.controller.getStaticConf()
				.getHeartBeatTimeout();
	}

	/**
	 * 收到其他节点发送来的领导者状态请求消息
	 *
	 * @param requestMessage
	 */
	public synchronized void receiveLeaderStatusRequestMessage(LeaderStatusRequestMessage requestMessage) {
		LOGGER.info("I am {}, receive leader status request for [{}:{}] !",
				tomLayer.controller.getStaticConf().getProcessId(), requestMessage.getSender(),
				requestMessage.getSequence());
		LeaderStatusResponseMessage responseMessage = new LeaderStatusResponseMessage(
				tomLayer.controller.getStaticConf().getProcessId(), requestMessage.getSequence(), tomLayer.leader(),
				tomLayer.getSynchronizer().getLCManager().getLastReg(), getLeaderStatus());
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
	}

	/**
	 * 处理收到的应答消息
	 *
	 * @param responseMessage
	 */
	public void receiveLeaderStatusResponseMessage(LeaderStatusResponseMessage responseMessage) {
		LeaderTimeoutTask timeoutTask = leaderTimeoutTask;
		if (timeoutTask != null) {
			timeoutTask.receiveLeaderStatusResponseMessage(responseMessage);
		}
	}

	/**
	 * 检查本地节点领导者状态
	 *
	 * @return
	 */
	public LeaderStatus getLeaderStatus() {
		if (inactived) {
			return LeaderStatus.TIMEOUT;
		}
		if (tomLayer.leader() == tomLayer.controller.getStaticConf().getProcessId()) {
			// 如果我是Leader，返回正常
			return LeaderStatus.OK;
		}
		// 需要判断所有连接是否已经成功建立
		if (!tomLayer.isConnectRemotesOK()) {
			// 流程还没有处理完的话，也返回成功
			return LeaderStatus.OK;
		}
		// 判断时间
		if (isHeartBeatTimeout()) {
			// 此处触发超时
			return LeaderStatus.TIMEOUT;
		}
		return LeaderStatus.OK;
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
	 * 在领导者节点上运行的心跳广播任务；
	 * <p>
	 * 该任务以配置文件指定的“心跳周期时长（HeartBeatPeriod）”为间隔周期性地广播心跳消息；
	 */
	static class LeaderHeartbeatBroadcastingTask implements Runnable {

		private static Logger LOGGER = LoggerFactory.getLogger(LeaderHeartbeatBroadcastingTask.class);

		private final HeartBeatTimer HEART_BEAT_TIMER;

		public LeaderHeartbeatBroadcastingTask(HeartBeatTimer heartBeatTimer) {
			this.HEART_BEAT_TIMER = heartBeatTimer;
		}

		@Override
		public void run() {
			// 判断是否是Leader
			if (!HEART_BEAT_TIMER.tomLayer.isLeader()) {
				return;
			}

			// 作为 Leader 发送心跳信息；
			int currentProcessId = HEART_BEAT_TIMER.getCurrentProcessId();
			try {
				if (!HEART_BEAT_TIMER.tomLayer.isConnectRemotesOK()) {
					return;
				}
				// 如果是Leader则发送心跳信息给其他节点，当前节点除外
				int currentRegency = HEART_BEAT_TIMER.tomLayer.getSynchronizer().getLCManager().getLastReg();
				HeartBeatMessage heartBeatMessage = new HeartBeatMessage(currentProcessId, currentProcessId,
						currentRegency);

				int[] followers = HEART_BEAT_TIMER.tomLayer.controller.getCurrentViewOtherAcceptors();
				HEART_BEAT_TIMER.tomLayer.getCommunication().send(followers, heartBeatMessage);
			} catch (Exception e) {
				// 处理异常，避免出错退出；
				LOGGER.warn("Error occurred while broadcasting heartbeat message from process[" + currentProcessId
						+ "]! --" + e.getMessage(), e);
			}
		}
	}

	/**
	 * 在非领导者节点上检查心跳超时的定时任务；
	 * <p>
	 * 
	 * 该任务以配置文件指定的“心跳超时时长”为周期执行；
	 *
	 */
	private class FollowerHeartbeatCheckingTask implements Runnable {

		@Override
		public void run() {
			// 再次判断是否是Leader；
			if (tomLayer.isLeader()) {
				return;
			}
			// 作为 Follower 节点处理消息；
			// 检查收到的InnerHeartBeatMessage是否超时
			// 需要判断所有连接是否已经成功建立
			if (!tomLayer.isConnectRemotesOK()) {
				return;
			}
			// 判断时间是否超时；
			if (isHeartBeatTimeout()) {
				// 触发超时；
				LOGGER.info("I am proc {} trigger hb timeout, time = {}, last hb time = {}",
						tomLayer.controller.getStaticConf().getProcessId(), System.currentTimeMillis(),
						heartBeatting.getTime());

				if (leaderTimeoutTask != null) {
					// 实际上不可能执行到此处：
					// 由于采用了 ScheduledExecutorService 的 scheduleWithFixedDelay 方式调度此任务，
					// 保证了不会在未完成上一次超时任务之前重复执行；
					LOGGER.error("There is a risky repeatedly scheduling of leader timeout checking task!");
					return;
				}

				try {
					leaderTimeoutTask = new LeaderTimeoutTask(HeartBeatTimer.this, tomLayer);
					Future<?> future = leaderTimeoutTask.start();
					future.get();
				} catch (CancellationException e) {
					LOGGER.warn("LeaderTimeoutTask is cancelled." + e.getMessage(), e);
				} catch (Exception e) {
					// 捕捉所有异常，防止异常抛出后终止心跳超时检测任务；
					LOGGER.error("Error occurred while running LeaderTimeoutTask! --" + e.getMessage(), e);
				} finally {
					leaderTimeoutTask = null;
				}
			}
		}// End of : public void run();
	}// End of : class FollowerHeartbeatCheckingTask;

	private static class HeartBeating {

		private long time;

		private LeaderRegency regency;

		private int from;

		public HeartBeating(LeaderRegency regency, int from, long time) {
			this.regency = regency;
			this.from = from;
			this.time = time;
		}

		public long getTime() {
			return time;
		}

		public LeaderRegency getRegency() {
			return regency;
		}

		public int getFrom() {
			return from;
		}
	}

	private static class NewLeader {

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

}
