package bftsmart.tom.leaderchange;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.reconfiguration.views.View;
import bftsmart.tom.core.TOMLayer;

class LeaderTimeoutTask {

	private static Logger LOGGER = LoggerFactory.getLogger(LeaderTimeoutTask.class);

	private final HeartBeatTimer HEART_BEAT_TIMER;
	
	private final TOMLayer tomLayer;

	public LeaderTimeoutTask(HeartBeatTimer heartbeatTimer, TOMLayer tomLayer) {
		this.HEART_BEAT_TIMER = heartbeatTimer;
		this.tomLayer = tomLayer;
	}

	public Future<?> start() {
		// 检查是否确实需要进行领导者切换
		// 首先发送其他节点领导者切换的消息，然后等待
		long startTs = System.currentTimeMillis();

		// 可以开始处理
		// 首先生成领导者状态请求消息
		LeaderStatusRequestMessage requestMessage = new LeaderStatusRequestMessage(
				tomLayer.controller.getStaticConf().getProcessId(), startTs);
		LOGGER.info("I am {}, send leader status request[{}] to others !", requestMessage.getSender(),
				requestMessage.getSequence());

		HEART_BEAT_TIMER.leaderStatusContext = new LeaderStatusContext(startTs, HEART_BEAT_TIMER);
		// 调用发送线程
		tomLayer.getCommunication()
				.send(tomLayer.controller.getCurrentViewOtherAcceptors(), requestMessage);

		try {
			// 等待应答结果
//					Map<Integer, LeaderStatus> statusMap = leaderStatusContext
//							.waitLeaderStatuses(LEADER_STATUS_MAX_WAIT);
			// 状态不为空的情况下进行汇总，并且是同一个Sequence
			if (HEART_BEAT_TIMER.leaderStatusContext.waitLeaderStatuses(LEADER_STATUS_MAX_WAIT)) {
				LOGGER.info("I am {}, receive more than f response for timeout, so I will start to LC !",
						tomLayer.controller.getStaticConf().getProcessId());
				// 再次进行判断，防止LC已处理完成再次触发
//                            if (!isLeaderChangeRunning) {
//                                startLeaderChange();
				// 表示可以触发领导者切换流程

				// 对于不一致的leaderid, regency进行本地更新，达到节点之间的一致性
//						checkAndUpdateLeaderInfos(statusMap);
				LeaderRegencyPropose regencyPropose = HEART_BEAT_TIMER.leaderStatusContext.generateRegencyPropose();

				tomLayer.requestsTimer.run_lc_protocol(regencyPropose);
//                     }
			}
		} catch (Exception e) {
			LOGGER.info("Handle leader status response error !!!", e);
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
		
			long sequence = leaderStatusContext.getSequence();
			// 将状态加入到其中
			LeaderStatus leaderStatus = leaderStatus(responseMessage);
			boolean valid = leaderStatusContext.addStatus(responseMessage.getSender(), leaderStatus, sequence);
			if (!valid) {
				LOGGER.warn("I am {}, receive leader status response[{}:{}] is not match !!!",
						tomLayer.controller.getStaticConf().getProcessId(), responseMessage.getSender(), sequence);
			}
	}

	private class LeaderStatusContext {

		private long sequence;

		private CountDownLatch latch = new CountDownLatch(1);

		private Map<Integer, LeaderStatus> leaderStatuses = new ConcurrentHashMap<>();

		private final HeartBeatTimer HEART_BEAT_TIMER;

		public LeaderStatusContext(long sequence, HeartBeatTimer heartbeatTimer) {
			this.sequence = sequence;
			this.HEART_BEAT_TIMER = heartbeatTimer;
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
		public synchronized boolean addStatus(int sender, LeaderStatus leaderStatus, long sequence) {
			if (this.sequence == sequence) {
				// 将状态加入到其中
				leaderStatuses.put(sender, leaderStatus);

				if (isStatusTimeout()) {
					latch.countDown();
				}

				return true;
			} else {
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

		public synchronized LeaderRegencyPropose generateRegencyPropose() {
			if (!isStatusTimeout()) {
				throw new IllegalStateException("Cann't generate regency propose without timeout!");
			}
			int maxRegency = tomLayer.getSynchronizer().getLCManager().getLastReg();
			for (Map.Entry<Integer, LeaderStatus> entry : leaderStatuses.entrySet()) {
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