package bftsmart.tom.leaderchange;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.reconfiguration.views.View;
import bftsmart.tom.core.Synchronizer;
import bftsmart.tom.core.TOMLayer;

/**
 * 领导者同步任务；
 * <p>
 * 
 * 当发现收到的领导者心跳执政期异常时，触发领导者同步任务向其它节点询问领导者信息，并更新同步后的结果；
 * <p>
 * 
 * @author huanghaiquan
 *
 */
public class LeaderConfirmationTask {

	private static final Logger LOGGER = LoggerFactory.getLogger(LeaderConfirmationTask.class);

	/**
	 * 任务最长的超时时长；
	 */
	private final long taskTimeout;

	private HeartBeatTimer hearbeatTimer;

	private TOMLayer tomLayer;

	private Synchronizer synchronizer;

	private final long startTimestamp;

	private ScheduledExecutorService executor;

	private Map<Integer, LeaderRegency> responsedRegencies;

	private View currentView;

	private volatile ScheduledFuture<?> taskFuture;

	/**
	 * @param taskTimeout   任务结束的最大超时时长；
	 * @param hearbeatTimer 系统中的心跳定时器；
	 * @param tomLayer      通讯层接口；
	 */
	public LeaderConfirmationTask(long taskTimeout, HeartBeatTimer hearbeatTimer, TOMLayer tomLayer) {
		this.taskTimeout = taskTimeout;
		this.hearbeatTimer = hearbeatTimer;
		this.tomLayer = tomLayer;
		this.synchronizer = tomLayer.getSynchronizer();
		this.currentView = this.tomLayer.controller.getCurrentView();
		this.startTimestamp = System.currentTimeMillis();

		this.executor = Executors.newSingleThreadScheduledExecutor();
		this.responsedRegencies = new ConcurrentHashMap<>();
	}

	public synchronized void start(long delay) {
		if (taskFuture != null) {
			return;
		}
		// 如果当前节点是 Leader 节点，先停止心跳；
		if (tomLayer.isLeader()) {
			hearbeatTimer.stopAll();
		}

		// 加入自己当前的执政期；
		responsedRegencies.put(getCurrentProcessId(), synchronizer.getLCManager().getCurrentRegency());

		// 先启动接收任务；
		taskFuture = scheduleResponseReciever(delay);

		// 发送领导者询问请求；
		sendLeaderRequestMessage(startTimestamp);
	}

	private ScheduledFuture<?> scheduleResponseReciever(long delay) {
		return executor.scheduleWithFixedDelay(new LeaderResponseWaiting(), delay, 2000L, TimeUnit.MILLISECONDS);
	}

	private boolean isStopped() {
		return taskFuture == null;
	}

	/**
	 * 只发送当前视图中未回复的；
	 * 
	 * @param sequence
	 */
	private void sendLeaderRequestMessage(long sequence) {
		// 向未收到的节点重复发送请求；
		int[] processIds = currentView.getProcesses();
		List<Integer> targetList = new ArrayList<>();
		for (int i = 0; i < processIds.length; i++) {
			if (responsedRegencies.containsKey(processIds[i])) {
				continue;
			}
			targetList.add(processIds[i]);
		}

		int[] targets = new int[targetList.size()];
		for (int i = 0; i < targetList.size(); i++) {
			targets[i] = targetList.get(i);
		}

		LeaderRequestMessage requestMessage = new LeaderRequestMessage(getCurrentProcessId(), sequence);
		tomLayer.getCommunication().send(targets, requestMessage);
	}

	/**
	 * 获取当前线程；
	 * 
	 * @return
	 */
	private int getCurrentProcessId() {
		return tomLayer.controller.getStaticConf().getProcessId();
	}

	/**
	 * 收到领导者应答请求
	 * 
	 * @param leaderRegencyResponse
	 */
	public synchronized void receiveLeaderResponseMessage(LeaderResponseMessage leaderRegencyResponse) {
		if (isStopped()) {
			return;
		}
		// 判断是否是自己发送的sequence
		long msgSequence = leaderRegencyResponse.getSequence();
		if (msgSequence != startTimestamp) {
			// 收到的心跳信息有问题，打印日志
			LOGGER.warn(
					"Receved a invalid LeaderReponseMessage with mismatched sequence! --[ExpectedSequence={}][MessageSequence={}][CurrentProcessId={}][MessageSender={}]",
					startTimestamp, msgSequence, getCurrentProcessId(), leaderRegencyResponse.getSender());
			return;
		}

		// 如果当前已经处于领导者选举进程中，则不作处理，并取消此任务；
		if (synchronizer.getLCManager().isInProgress()) {
			cancelTask();
			return;
		}

		// 是当前节点发送的请求，则将其加入到Map中
		responsedRegencies.put(leaderRegencyResponse.getSender(),
				new LeaderRegency(leaderRegencyResponse.getLeader(), leaderRegencyResponse.getLastRegency()));

		// 计算最高执政期的列表；
		List<LeaderRegency> greatestRegencies = countGreatestRegencies();
		int quorum = tomLayer.controller.getStaticConf().isBFT() ? currentView.computeBFT_QUORUM()
				: currentView.computeCFT_QUORUM();
		if (greatestRegencies.size() >= quorum) {
			// 符合法定数量；
			// 尝试跃迁到新的执政期；
			LeaderRegency newRegency = greatestRegencies.get(0);
			LeaderRegency currentRegency = synchronizer.getLCManager().getCurrentRegency();
			if (synchronizer.getLCManager().tryJumpToRegency(newRegency)) {
				tomLayer.execManager.setNewLeader(newRegency.getLeaderId());
				tomLayer.getSynchronizer().removeSTOPretransmissions(newRegency.getId());
			} else {
				LOGGER.warn("The regency could not jumps from [Regency={},Leader={}] to {Regency={},Leader={}}!",
						currentRegency.getId(), currentRegency.getLeaderId(), newRegency.getId(),
						newRegency.getLeaderId());
			}

			// 结束当前任务；
			cancelTask();

			// 恢复心跳定时器；
			resumeHeartBeatTimer();
		}
	}

	private void resumeHeartBeatTimer() {
		// TODO Auto-generated method stub
		// 如果当前页不在选举进程中，则恢复心跳进程；
		// 如果处于选举进程中，则此处不必恢复，当选举进程结束后会自行恢复；
		if (synchronizer.getLCManager().isInProgress()) {
			return;
		}

		hearbeatTimer.start();
	}

	private synchronized void cancelTask() {
		taskFuture.cancel(true);
		taskFuture = null;
		onCompleted();
	}

	protected void onCompleted() {
	}

	/**
	 * 返回最高的执政期列表；
	 * 
	 * @return
	 */
	private List<LeaderRegency> countGreatestRegencies() {
		List<LeaderRegency> greatestRegencies = new ArrayList<>();
		int regencyId = -1;
		for (LeaderRegency reg : responsedRegencies.values()) {
			if (reg.getId() > regencyId) {
				greatestRegencies.clear();
				greatestRegencies.add(reg);
				regencyId = reg.getId();
				continue;
			}
			if (reg.getId() == regencyId) {
				greatestRegencies.add(reg);
			}
		}
		return greatestRegencies;
	}

	private boolean isTaskTimeout() {
		return (System.currentTimeMillis() - startTimestamp) > taskTimeout;
	}

	/**
	 * @author huanghaiquan
	 *
	 */
	private class LeaderResponseWaiting implements Runnable {

		@Override
		public void run() {
			try {
				if (synchronizer.getLCManager().isInProgress()) {
					cancelTask();
					return;
				}

				// 如果已经超时，且尚未完成任务，则终止任务，并回复心跳定时器；
				if (isTaskTimeout()) {
					cancelTask();
					resumeHeartBeatTimer();
				}

				// 向未回复的节点重复发请求；
				sendLeaderRequestMessage(startTimestamp);
			} catch (Exception e) {
				// 捕捉错误，避免终端定时任务；
				LOGGER.error("Error occurred while waiting leader responses! --" + e.getMessage(), e);
			}
		}
	}
}
