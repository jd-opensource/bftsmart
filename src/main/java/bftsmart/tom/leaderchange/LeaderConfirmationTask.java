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

	private volatile boolean selfVoted = false;

	private long startTimestamp;

	private ScheduledExecutorService executor;

	private Map<Integer, LeaderRegencyView> responsedRegencies;

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
		this.currentView = this.tomLayer.controller.getCurrentView();
		this.startTimestamp = System.currentTimeMillis();

		this.executor = Executors.newSingleThreadScheduledExecutor();
		this.responsedRegencies = new ConcurrentHashMap<>();
	}

	public synchronized void start(long delay) {
		if (taskFuture != null) {
			return;
		}

		LOGGER.debug("Start the leader confirm task ...[CurrentId={}][ExecLeaderId={}][StartTime={}]",
				tomLayer.getCurrentProcessId(), tomLayer.getExecManager().getCurrentLeader(), startTimestamp);

		// 如果当前节点是 Leader 节点，先停止心跳；
		if (tomLayer.isLeader()) {
			hearbeatTimer.stopAll();
		}

		// 先启动接收任务；
		taskFuture = scheduleResponseReciever(delay);
	}

	private ScheduledFuture<?> scheduleResponseReciever(long delay) {
		return executor.scheduleWithFixedDelay(new LeaderResponseWaiting(), delay, 2000L, TimeUnit.MILLISECONDS);
	}

	private boolean isStopped() {
		return taskFuture == null;
	}

	/**
	 * 返回尚未回复领导者查询请求的节点 Id 列表；
	 * 
	 * @return
	 */
	private int[] getUnresponsedProcessIds() {
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
		return targets;
	}

	/**
	 * 只发送当前视图中未回复的；
	 * 
	 * @param sequence
	 */
	private void sendLeaderRequestMessage(long sequence) {
		// 向未收到的节点重复发送请求；
		int[] targets = getUnresponsedProcessIds();

		LeaderRequestMessage requestMessage = new LeaderRequestMessage(getCurrentProcessId(), sequence);

		LOGGER.debug("Send leader request message ...[CurrentId={}][ExecLeaderId={}][Regency={}][Sequence={}]",
				tomLayer.getCurrentProcessId(), tomLayer.getExecManager().getCurrentLeader(),
				tomLayer.getSynchronizer().getLCManager().getCurrentRegency(), sequence);

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
	 * 收到领导者应答;
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
		if (tomLayer.getSynchronizer().getLCManager().isInProgress()) {
			cancelTask();
			return;
		}

		// 是当前节点发送的请求，则将其加入到Map中
//		LeaderRegencyPropose respPropose = LeaderRegencyPropose.copy(leaderRegencyResponse.getLeaderId(),
//				leaderRegencyResponse.getLastRegency(), leaderRegencyResponse.getViewId(),
//				leaderRegencyResponse.getViewProcessIds(), leaderRegencyResponse.getSender());
		responsedRegencies.put(leaderRegencyResponse.getSender(), leaderRegencyResponse);

	}

	/**
	 * 处理回复，尝试完成领导者确认；
	 * <p>
	 * 如果成功获得有效结果，已不需要继续执行任务，则返回 true；
	 * 
	 * @return
	 */
	private boolean tryComplete() {
		// 计算最高执政期的列表；
		ElectionResult electionResult = ElectionResult.generateHighestRegnecy(responsedRegencies.values());

		// 排除掉不处于NORMAL状态的节点提议
		int validStateCount = getValidStateCountFromElectionResult(electionResult);

		int quorum = tomLayer.controller.getStaticConf().isBFT() ? currentView.computeBFT_QUORUM()
				: currentView.computeCFT_QUORUM();

		// 如果符合法定数量；
		// 尝试跃迁到新的执政期；
		// 符合法定数量的情形包括两种可能：
		// 1. 已存在的节点回复已经满足法定数量；
		// 2. 已存在的节点回复的数量比法定数量少 1 个，如果加入当前节点则刚好维护法定数量的网络活性；

		LeaderRegency newRegency = null;
		if (validStateCount >= quorum) {
			// 1. 已存在的节点回复已经满足法定数量；
			newRegency = electionResult.getRegency();
		} else if (validStateCount + 1 == quorum) {
			// 2. 已存在的节点回复的数量比法定数量少 1 个;
			// 如果新的领导者属于已存在的节点或者当前节点，则加入当前节点；
			int newLeader = electionResult.getRegency().getLeaderId();
			// Proposer发送者认为的领导者不一定在所有Prooposer发送者中间，有可能领导者此时处于宕机状态
			if (electionResult.containsProposer(newLeader)) {
				newRegency = electionResult.getRegency();
			}
		}

		if (newRegency != null) {
			LeaderRegency currentRegency = tomLayer.getSynchronizer().getLCManager().getCurrentRegency();
			if (currentRegency.getId() == newRegency.getId()
					&& currentRegency.getLeaderId() == newRegency.getLeaderId()) {
				// 与本地的领导者执政期完全相等；
				hearbeatTimer.setLeaderActived();
			} else if (tomLayer.getSynchronizer().getLCManager().tryJumpToRegency(newRegency)) {
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

			return true;
		}
		return false;
	}

	private int getValidStateCountFromElectionResult(ElectionResult electionResult) {

		int validStateCount = 0;

		for (LeaderRegencyView leaderRegencyView : electionResult.getValidProposes()) {
			if (leaderRegencyView.getStateCode() == LCState.NORMAL.CODE) {
				validStateCount++;
			}
		}

		return validStateCount;
	}

	/**
	 * 根据收集的全局的执政期清单，以当前视图为基准生成新的执政期提议；
	 * 
	 * @return
	 */
	public synchronized LeaderRegencyPropose generateRegencyPropose() {
		int maxRegency = tomLayer.getSynchronizer().getLCManager().getLastReg();
		for (Map.Entry<Integer, LeaderRegencyView> entry : responsedRegencies.entrySet()) {
			int regency = entry.getValue().getRegency().getId();
			if (regency > maxRegency) {
				maxRegency = regency;
			}
		}
		int nextRegency = maxRegency + 1;
		View view = tomLayer.controller.getCurrentView();
		int sender = tomLayer.controller.getStaticConf().getProcessId();

		return LeaderRegencyPropose.chooseFromView(nextRegency, view, sender);
	}

	private void resumeHeartBeatTimer() {
		// TODO Auto-generated method stub
		// 如果当前页不在选举进程中，则恢复心跳进程；
		// 如果处于选举进程中，则此处不必恢复，当选举进程结束后会自行恢复；
		if (tomLayer.getSynchronizer().getLCManager().isInProgress()) {
			return;
		}

		hearbeatTimer.start();
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

			LOGGER.debug("Quit the leader confirm task! --[CurrentId={}][ExecLeaderId={}][Sequence={}]",
					tomLayer.getCurrentProcessId(), tomLayer.getExecManager().getCurrentLeader(), startTimestamp);
			tomLayer.setLeaderConfirmed(true);

			onCompleted();
		}
	}

	protected void onCompleted() {
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
				if (!tomLayer.isRunning()) {
					// 系统已经关闭，直接退出；
					cancelTask();
					return;
				}

				if (!tomLayer.isConnectRemotesOK()) {
					// 状态传输可能还未结束，等待状态传输结束再继续处理；
					// 并防止任务超时；
					startTimestamp = System.currentTimeMillis();
					return;
				}

				if (tomLayer.getSynchronizer().getLCManager().isInProgress()) {
					cancelTask();
					return;
				}

				// 如果已经超时，且尚未完成任务，则终止任务，发起超时；
				if (isTaskTimeout()) {
					// 有可能该任务运行的时候，由于收到足够数量的stop而附议，完成一轮领导者切换并启动了定时器，此时不必再次发起lc流程
					if (!hearbeatTimer.isActived()) {
//						hearbeatTimer.setLeaderInactived();
						LeaderRegencyPropose propose = generateRegencyPropose();
						tomLayer.getRequestsTimer().run_lc_protocol(propose);
					}
					cancelTask();
					return;
				}

				if (!selfVoted) {
					// 加入自己当前的执政期；
					LeaderRegency currentRegency = tomLayer.getSynchronizer().getLCManager().getCurrentRegency();
					LeaderRegencyPropose selfPropose = LeaderRegencyPropose.copy(currentRegency.getLeaderId(),
							currentRegency.getId(), tomLayer.controller.getCurrentViewId(),
							tomLayer.controller.getCurrentViewProcesses(), getCurrentProcessId());
					responsedRegencies.put(getCurrentProcessId(), selfPropose);
					selfVoted = true;
				}

				boolean completed = tryComplete();
				if (!completed) {
					// 向未回复的节点重复发请求；
					sendLeaderRequestMessage(startTimestamp);
				}
			} catch (Exception e) {
				// 捕捉错误，避免终端定时任务；
				LOGGER.error("Error occurred while waiting leader responses! --" + e.getMessage(), e);
			}
		}
	}
}
