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

import com.jd.blockchain.utils.ArrayUtils;

import bftsmart.reconfiguration.views.View;
import bftsmart.tom.core.Synchronizer;
import bftsmart.tom.core.TOMLayer;
import bftsmart.tom.leaderchange.HeartBeatTimer.NewLeader;

/**
 * 领导者同步任务；
 * <p>
 * 
 * 当发现收到的领导者心跳执政期异常时，触发领导者同步任务向其它节点询问领导者信息，并更新同步后的结果；
 * 
 * @author huanghaiquan
 *
 */
public class LeaderConfirmationTask {

	private static final Logger LOGGER = LoggerFactory.getLogger(LeaderConfirmationTask.class);

	private HeartBeatTimer hearbeatTimer;

	private TOMLayer tomLayer;

	private Synchronizer synchronizer;

	/**
	 * 有争议的心跳执政期；
	 */
	private LeaderRegency beatingRegency;

	private final long startTimestamp;

	private ScheduledExecutorService executor;

	private Map<Integer, LeaderRegency> responsedRegencies;

	private View currentView;

	private ScheduledFuture<?> future;

	/**
	 * @param beatingRegency 有争议的心跳执政期；
	 * @param hearbeatTimer
	 * @param tomLayer
	 */
	public LeaderConfirmationTask(LeaderRegency beatingRegency, HeartBeatTimer hearbeatTimer, TOMLayer tomLayer) {
		this.beatingRegency = beatingRegency;
		this.hearbeatTimer = hearbeatTimer;
		this.tomLayer = tomLayer;
		this.synchronizer = tomLayer.getSynchronizer();
		this.currentView = this.tomLayer.controller.getCurrentView();
		this.startTimestamp = System.currentTimeMillis();

		this.executor = Executors.newSingleThreadScheduledExecutor();
		this.responsedRegencies = new ConcurrentHashMap<>();
	}

	public synchronized void start() {
		if (future != null) {
			return;
		}
		// 如果当前节点是 Leader 节点，先停止心跳；
		if (tomLayer.isLeader()) {
			hearbeatTimer.stopAll();
		}

		// 加入自己当前的执政期；
		responsedRegencies.put(getCurrentProcessId(), synchronizer.getLCManager().getCurrentRegency());

		// 先启动接收任务；
		future = scheduleResponseReciever();

		// 发送领导者询问请求；
		sendLeaderRequestMessage(startTimestamp);
	}

	private ScheduledFuture<?> scheduleResponseReciever() {
		return executor.scheduleWithFixedDelay(new LeaderResponseReceiver(), 0, 1000L, TimeUnit.MILLISECONDS);
	}

	/**
	 * 只发送当前视图中未回复的；
	 * 
	 * @param sequence
	 */
	private void sendLeaderRequestMessage(long sequence) {
		// 向未收到的节点发送请求；
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

	private int getCurrentProcessId() {
		return tomLayer.controller.getStaticConf().getProcessId();
	}

	/**
	 * 收到领导者应答请求
	 * 
	 * @param leaderRegencyResponse
	 */
	public void receiveLeaderResponseMessage(LeaderResponseMessage leaderRegencyResponse) {
		// 判断是否是自己发送的sequence
		long msgSequence = leaderRegencyResponse.getSequence();
		if (msgSequence != startTimestamp) {
			// 收到的心跳信息有问题，打印日志
			LOGGER.warn(
					"Receved a invalid LeaderReponseMessage with mismatched sequence! --[ExpectedSequence={}][MessageSequence={}][CurrentProcessId={}][MessageSender={}]",
					startTimestamp, msgSequence, getCurrentProcessId(), leaderRegencyResponse.getSender());
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
			
		}

		// 判断收到的心跳信息是否满足
		NewLeader newLeader = newLeader(leaderRegencyResponse.getSequence());
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

	private class LeaderResponseReceiver implements Runnable {

		@Override
		public void run() {
			NewLeader newLeader = newLeader(currentSequence);
			if (newLeader != null) {
				// 满足，则更新当前节点的Leader
				tomLayer.execManager.setNewLeader(newLeader.getNewLeader());
				tomLayer.getSynchronizer().getLCManager().setNextReg(newLeader.getLastRegency());
				tomLayer.getSynchronizer().getLCManager().setLastReg(newLeader.getLastRegency());
			}
		}
	}
}
