package bftsmart.tom.leaderchange;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.reconfiguration.views.View;
import bftsmart.tom.core.TOMLayer;

class LeaderTimeoutTask {

	private static Logger LOGGER = LoggerFactory.getLogger(LeaderTimeoutTask.class);

	private static final long LEADER_STATUS_MAX_WAIT = 15000;

	private final HeartBeatTimer HEART_BEAT_TIMER;

	private final TOMLayer tomLayer;

	private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

	private LeaderStatusContext leaderStatusContext;

	private ScheduledFuture<?> taskFuture;

	private long startTimestamp;

	public LeaderTimeoutTask(HeartBeatTimer heartbeatTimer, TOMLayer tomLayer) {
		this.HEART_BEAT_TIMER = heartbeatTimer;
		this.tomLayer = tomLayer;
	}

	public synchronized Future<?> start() {
		if (taskFuture != null) {
			return taskFuture;
		}

		// 检查是否确实需要进行领导者切换
		// 首先发送其他节点领导者切换的消息，然后等待
		startTimestamp = System.currentTimeMillis();

		LOGGER.debug("Start the timeout task ...[CurrentId={}][Sequence={}]", tomLayer.getCurrentProcessId(),
				startTimestamp);

		// 可以开始处理
		long sequence = startTimestamp;
		leaderStatusContext = new LeaderStatusContext(sequence);

		// 加入自己的状态；
		setSelfStatus();

		// 生成领导者状态请求消息
		sendStatusRequest(startTimestamp);

		taskFuture = executor.scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
				waitStatusResposne();
			}
		}, 0, 2000, TimeUnit.MILLISECONDS);

		return taskFuture;
	}

	private void setSelfStatus() {
		if (!tomLayer.isRunning()) {
			cancelTask();
			return;
		}
		LeaderRegency currentRegency = tomLayer.getSynchronizer().getLCManager().getCurrentRegency();
		LeaderStatus status = HEART_BEAT_TIMER.getLeaderStatus();
		LeaderRegencyStatus timeoutStatus = new LeaderRegencyStatus(status, currentRegency.getLeaderId(),
				currentRegency.getId());
		leaderStatusContext.addStatus(tomLayer.getCurrentProcessId(), timeoutStatus, startTimestamp);
	}

	private synchronized void cancelTask() {
		ScheduledFuture<?> future = taskFuture;
		taskFuture = null;
		if (future != null) {
			future.cancel(true);

			try {
				executor.shutdown();
			} catch (Exception e) {
			}

			onCompleted();
			
			LOGGER.debug("Quit the timeout task! [CurrentId={}]", tomLayer.getCurrentProcessId());
		}
	}

	private boolean isTaskTimeout() {
		return (System.currentTimeMillis() - startTimestamp) > LEADER_STATUS_MAX_WAIT;
	}

	private void resetTaskTimeout() {
		startTimestamp = System.currentTimeMillis();
	}

	/**
	 * 等待超时状态查询的回复；
	 */
	private void waitStatusResposne() {
		try {
			if (tomLayer.getSynchronizer().getLCManager().isInProgress()) {
				// 已经在选举中了，先终止当前任务；
				cancelTask();
				return;
			}

			if (!HEART_BEAT_TIMER.isHeartBeatTimeout()) {
				// 已经不超时了，则终止当前任务；
				cancelTask();
				return;
			}

			// 等待应答结果
			// 状态不为空的情况下进行汇总，并且是同一个Sequence
			View currentView = tomLayer.controller.getCurrentView();
			int quorum = tomLayer.controller.getStaticConf().isBFT() ? currentView.computeBFT_QUORUM()
					: currentView.computeCFT_QUORUM();

			// 检查是否全局已经进入超时状态；
			LeaderRegencyStatus[] timeoutStatuses = leaderStatusContext.getTimeoutStatus();
			if (timeoutStatuses.length >= quorum) {
				// 超时数量已经超过法定数量；
				LOGGER.info("I am {}, receive more than f response for timeout, so I will start to LC !",
						tomLayer.controller.getStaticConf().getProcessId());

				// 表示可以触发领导者切换流程
				LeaderRegencyPropose regencyPropose = leaderStatusContext.generateRegencyPropose();
				tomLayer.requestsTimer.run_lc_protocol(regencyPropose);

				// 取消任务；
				cancelTask();
				return;
			}

			// 未达到全局超时的状态；
			// 检查是否全局的法定大多数节点存在一致的执政期；

			LeaderRegencyStatus[] greatestRegencyStatuses = leaderStatusContext.getGreatestRegencyStatus();
			if (greatestRegencyStatuses.length >= quorum) {
				// 全局大多数节点已经具备一致的执政期；当前节点跟上新的领导者；
				boolean leaderConsistency = true;
				int leaderId = greatestRegencyStatuses[0].getLeaderId();
				for (int i = 1; i < greatestRegencyStatuses.length; i++) {
					if (leaderId != greatestRegencyStatuses[i].getLeaderId()) {
						// 出现相同的执政期但是领导者不同的情况；
						// 此种情况先不做处理，有可能是其它节点处于不稳定状态；
						// 继续等待到以后的机会；
						leaderConsistency = false;
					}
				}
				if (leaderConsistency) {
					// 尝试跳跃到新的执政期；
					// 如果未能跳跃到新的执政期，则继续执行超时等待任务，等待以后的机会；
					LeaderRegency newRegency = greatestRegencyStatuses[0];
					if (tomLayer.getSynchronizer().getLCManager().tryJumpToRegency(newRegency)) {
						tomLayer.getExecManager().setNewLeader(newRegency.getLeaderId());
						// 完成任务；
						cancelTask();
						return;
					}
				}
			}

			// 如果任务的执行时间超时了仍然未能等到退出条件；
			// 则继续重复发送超时状态请求，并继续接收其它节点的回复，不断更新本地接收到全局的执政期状态；
			if (isTaskTimeout()) {
				// 更新自身的状态；
				setSelfStatus();
				// 发送超时状态请求；
				sendStatusRequest(leaderStatusContext.sequence);

				// 重置任务执行超时状态;
				// 控制重复发送超时状态请求的频率，只有每一次任务执行时间超时之后才重新发送；
				resetTaskTimeout();
			}

		} catch (Exception e) {
			// 处理错误；避免异常抛出后中断任务；
			LOGGER.info("Handle leader status response error !!!", e);
		}
	}

	private void sendStatusRequest(long startTs) {
		try {
			LeaderStatusRequestMessage requestMessage = new LeaderStatusRequestMessage(
					tomLayer.controller.getStaticConf().getProcessId(), startTs);

			LOGGER.info("I am {}, send leader status request[{}] to others !", requestMessage.getSender(),
					requestMessage.getSequence());

			// 调用发送线程
			tomLayer.getCommunication().send(tomLayer.controller.getCurrentViewOtherAcceptors(), requestMessage);
		} catch (Exception e) {
			// 发送消息的任务不允许抛出异常，影响上层的处理；
			LOGGER.error("Error occurred while sending timeout status request! --[CurrentProccesId="
					+ tomLayer.getCurrentProcessId() + "] " + e.getMessage(), e);
		}
	}

	protected void onCompleted() {
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

		// 将状态加入到其中
		LeaderRegencyStatus leaderStatus = resolveLeaderStatus(responseMessage);
		leaderStatusContext.addStatus(responseMessage.getSender(), leaderStatus, responseMessage.getSequence());
	}

	/**
	 * 领导者状态
	 *
	 * @param responseMessage
	 * @return
	 */
	private LeaderRegencyStatus resolveLeaderStatus(LeaderStatusResponseMessage responseMessage) {
		return new LeaderRegencyStatus(responseMessage.getStatus(), responseMessage.getLeaderId(),
				responseMessage.getRegency());
	}

	private class LeaderStatusContext {

		private final long sequence;

		private Map<Integer, LeaderRegencyStatus> leaderStatuses = new ConcurrentHashMap<>();

		public LeaderStatusContext(long sequence) {
			this.sequence = sequence;
		}

		/**
		 * 返回超时的状态清单；
		 * 
		 * @return
		 */
		public LeaderRegencyStatus[] getTimeoutStatus() {
			List<LeaderRegencyStatus> timeoutStatusList = new ArrayList<>();
			for (Map.Entry<Integer, LeaderRegencyStatus> entry : leaderStatuses.entrySet()) {
				if (entry.getValue().getStatus() == LeaderStatus.TIMEOUT) {
					timeoutStatusList.add(entry.getValue());
				}
			}
			return timeoutStatusList.toArray(new LeaderRegencyStatus[timeoutStatusList.size()]);
		}

		public LeaderRegencyStatus[] getGreatestRegencyStatus() {
			int regencyId = -1;
			List<LeaderRegencyStatus> timeoutStatusList = new ArrayList<>();
			for (Map.Entry<Integer, LeaderRegencyStatus> entry : leaderStatuses.entrySet()) {
				if (entry.getValue().getId() > regencyId) {
					timeoutStatusList.clear();
					timeoutStatusList.add(entry.getValue());
					continue;
				}
				if (entry.getValue().getId() == regencyId) {
					timeoutStatusList.add(entry.getValue());
					continue;
				}
			}
			return timeoutStatusList.toArray(new LeaderRegencyStatus[timeoutStatusList.size()]);
		}

		/**
		 * 记录指定节点的领导者状态；
		 * <p>
		 * 
		 * 如果加入的是有效的状态数据，即序列号相同，则返回 true；否则返回 false；
		 * 
		 * @param sender
		 * @param leaderStatus
		 * @param sequence
		 */
		private synchronized boolean addStatus(int sender, LeaderRegencyStatus leaderStatus, long sequence) {
			if (this.sequence == sequence) {
				// 将状态加入到其中
				leaderStatuses.put(sender, leaderStatus);
				return true;
			} else {
				LOGGER.warn("Node[{}] receive leader status response from node[{}] with wrong sequence[{}]!",
						tomLayer.controller.getStaticConf().getProcessId(), sender, sequence);
				return false;
			}
		}

//			private void reset() {
//				sequence = -1L;
//				leaderStatuses.clear();
//				latch = null;
//			}
		//
//			public synchronized void init(long sequence, int remoteSize) {
//				// 先进行重置
//				this.sequence = sequence;
//				
//			}

//		/**
//		 * 等待领导者状态查询结果的超时状态满足以下条件： <br>
//		 * 1. 查询返回的所有节点中（？？也包括自身节点），处于领导者心跳超时的节点的数量满足：大于等于(n + f)/2 ;
//		 * 
//		 * <p>
//		 * 方法会堵塞等待其它节点回复查询结果，最长等待指定的时长；
//		 * <p>
//		 * 
//		 * 如果不满足数量条件，或者超时，都返回 false； 只有满足数量条件的情况下，才返回 true；
//		 * 
//		 * @param waitMillSeconds
//		 * @return
//		 * @throws Exception
//		 */
//		public boolean waitLeaderStatuses(long waitMillSeconds) throws Exception {
//			latch.await(waitMillSeconds, TimeUnit.MILLISECONDS);
//			return isStatusTimeout();
//		}

//		private synchronized LeaderRegency[] generateGreatestRegencies() {
//			
//		}

		private synchronized boolean isUpToTimeoutQuorum() {
			// 以（n + f）/ 2作为判断依据
			// 增强判断的条件，防止不必要的LC流程,如果因为网络差收不到足够的status响应，或者没有足够的共识节点心跳超时都不会触发LC流程
			// 注意：取值不能从this.tomLayer.controller.getStaticConf()中获得，激活新节点时TOMconfig并没有更新，只更新了视图
			View currentView = tomLayer.controller.getCurrentView();

			int counterOfTimeout = 0;
			for (Map.Entry<Integer, LeaderRegencyStatus> entry : leaderStatuses.entrySet()) {
				if (entry.getValue().getStatus() == LeaderStatus.TIMEOUT) {
					counterOfTimeout++;
				}
			}
			int timeoutQuorum = tomLayer.controller.getStaticConf().isBFT() ? currentView.computeBFT_QUORUM()
					: currentView.computeCFT_QUORUM();

			LOGGER.info("Node[{}] is checking timeout... --[ReceiveTimeoutSize={}][TimeoutQuorum={}]",
					tomLayer.controller.getStaticConf().getProcessId(), counterOfTimeout, timeoutQuorum);
			return counterOfTimeout >= timeoutQuorum;
		}

		public synchronized LeaderRegencyPropose generateRegencyPropose() {
			if (!isUpToTimeoutQuorum()) {
				throw new IllegalStateException("Cann't generate regency propose without timeout!");
			}
			int maxRegency = tomLayer.getSynchronizer().getLCManager().getLastReg();
			for (Map.Entry<Integer, LeaderRegencyStatus> entry : leaderStatuses.entrySet()) {
				int leader = entry.getValue().getLeaderId();
				int regency = entry.getValue().getId();

				if (regency > maxRegency) {
					maxRegency = regency;
				}
			}
			int nextRegency = maxRegency + 1;
			View view = tomLayer.controller.getCurrentView();
			int sender = tomLayer.controller.getStaticConf().getProcessId();

			return LeaderRegencyPropose.chooseFromView(nextRegency, view, sender);

//				if (maxRegency < tomLayer.getSynchronizer().getLCManager().getLastReg()) {
//					return;
//				}

			/// ???
//				if (tomLayer.getExecManager().getCurrentLeader() != minLeader) {
//					tomLayer.getExecManager().setNewLeader(minLeader);
//					tomLayer.getSynchronizer().getLCManager().setNewLeader(minLeader);
//				}
//				tomLayer.getSynchronizer().getLCManager().setNextReg(maxRegency);
//				tomLayer.getSynchronizer().getLCManager().setLastReg(maxRegency);
		}
	}
}