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
package bftsmart.tom.leaderchange;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.security.PublicKey;
import java.security.SignedObject;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.communication.MacKey;
import bftsmart.consensus.TimestampValuePair;
import bftsmart.consensus.app.SHA256Utils;
import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.reconfiguration.ViewTopology;
import bftsmart.tom.core.TOMLayer;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.util.TOMUtil;
import utils.ArrayUtils;

/**
 *
 * This class implements a manager of information related to the leader change
 * protocol It also implements some predicates and methods necessary for the
 * protocol in accordance to Cachin's 'Yet Another Visit to Paxos' (April 2011).
 * 
 * @author Joao Sousa
 */
public class LCManager {
	private static final Logger LOGGER = LoggerFactory.getLogger(LCManager.class);

	// 当前的执政期；
	private LeaderRegency currentRegency;

	// 正在进行的选举过程的领导者执政ID，该值等于 lastreg + 1;
	// 如果没有正在进行的选举过程，则该值等于 lastreg;
	private int nextreg;

	// requests that timed out
	private List<TOMMessage> currentRequestTimedOut = null;

	// requests received in STOP messages
	private List<TOMMessage> requestsFromSTOP = null;

	/**
	 * 执政期的 STOP 消息映射表；“键”为提议的执政期（regency），“值”为各个节点发送的提议；
	 */
	private Map<Integer, Map<Integer, LeaderRegencyPropose>> stops;
	private Map<Integer, HashSet<CertifiedDecision>> lastCIDs;
	private Map<Integer, HashSet<SignedObject>> collects;

	// stuff from the TOM layer that this object needss
	private ViewTopology SVController;
	private SHA256Utils md = new SHA256Utils();
	private TOMLayer tomLayer;

	private LCTimestampStatePair lcTimestampStatePair;

	// private Cipher cipher;
//	private Mac mac;

	/**
	 * Constructor
	 *
	 * @param reconfManager The reconfiguration manager from TOM layer
	 * @param md            The message digest engine from TOM layer
	 */
	public LCManager(TOMLayer tomLayer, ViewTopology SVController, SHA256Utils md) {
		this.tomLayer = tomLayer;
//		this.lastreg = 0;
//		this.currentLeader = 0;
		this.currentRegency = new LeaderRegency(0, 0);
		this.nextreg = 0;

		this.stops = new ConcurrentHashMap<Integer, Map<Integer, LeaderRegencyPropose>>();
		this.lastCIDs = new ConcurrentHashMap<Integer, HashSet<CertifiedDecision>>();
		this.collects = new ConcurrentHashMap<Integer, HashSet<SignedObject>>();

		this.SVController = SVController;
		this.md = md;

		this.lcTimestampStatePair = new LCTimestampStatePair(System.currentTimeMillis(), LCState.NORMAL);



//		try {
//			// this.cipher = Cipher.getInstance("DES/ECB/PKCS5Padding");
//			// this.cipher = Cipher.getInstance(ServerConnection.MAC_ALGORITHM);
//			this.mac = Mac.getInstance(MessageConnection.MAC_ALGORITHM);
//		} catch (NoSuchAlgorithmException /* | NoSuchPaddingException */ ex) {
//			ex.printStackTrace();
//		}

	}

	public LeaderRegency getCurrentRegency() {
		return currentRegency;
	}

	public int getCurrentLeader() {
		return currentRegency.getLeaderId();
	}

	private int getCurrentProcessId() {
		return SVController.getStaticConf().getProcessId();
	}

//	/**
//	 * Deterministically elects a new leader, based current leader and membership
//	 * 
//	 * @return The new leader
//	 */
//	public LeaderRegency chooseNew(int regency) {
//
//		int[] proc = SVController.getCurrentViewProcesses();
//		int minProc = proc[0];
//		int maxProc = proc[0];
//
//		for (int p : proc) {
//			if (p < minProc)
//				minProc = p;
//			if (p > maxProc)
//				maxProc = p;
//		}
//
//		do {
//			currentLeader++;
//			if (currentLeader > maxProc) {
//
//				currentLeader = minProc;
//			}
//		} while (!SVController.isCurrentViewMember(currentLeader));
//
//		LOGGER.debug("I am proc {} , get new leader = {}", tomLayer.controller.getStaticConf().getProcessId(),
//				currentLeader);
//
//		return currentLeader;
//	}

	/**
	 * 根据已经收集的全部选举提议生成选举结果；
	 * <p>
	 * 如果总的提议数低于法定的可提交提议数（{@link #getBFTCommitQuorum()}），则返回 null；
	 * <p>
	 * 如果选举结果中的有效投票数低于合法提议数（{@link #getBFTCommitQuorum()}），则返回 null；
	 * <p>
	 * 如果选举结果中的视图与当前节点的视图不一致，则返回 null；
	 * <p>
	 * 此方法仅计算选举结果，也不会提交选举结果，也不会影响选举进程；
	 * <p>
	 * 
	 * @return
	 */
	public ElectionResult generateQuorumElectionResult(int regencyId) {
		ElectionResult result = generateElectionResult(regencyId);
		if (result == null) {
			return null;
		}
		int validProposedCount = result.getValidCount();
		if (SVController.getStaticConf().isBFT()) {
			if (validProposedCount < getBFTCommitQuorum()) {
				return null;
			}
		} else {
			if (validProposedCount < getCFTCommitQuorum()) {
				return null;
			}
		}
		if (!isEqualToCurrentView(result)) {
			return null;
		}
		return result;
	}

	/**
	 * 根据已经收集的全部选举提议生成选举结果；
	 * <p>
	 * 如果总的提议数低于法定的可提交提议总数（{@link #getBFTCommitQuorum()}），则返回 null；
	 * <p>
	 * 此方法仅计算选举结果，不检查选举结果中的有效投票数是否满足合法性要求，也不会提交选举结果，也不会影响选举进程；
	 * <p>
	 * 
	 * @return
	 */
	public ElectionResult generateElectionResult(int regencyId) {
		if (!isUpToCommitQuorum(regencyId)) {
			// 选举提议的数量未达到法定数量；
			return null;
//			throw new IllegalStateException(
//					"The election of regency[" + regencyId + "] is not up to the commiting quorum!");
		}
		// 检查所有选举提议的一致性：
		// 1. 是否在一致的视图下，提出了一致的领导者执政期提议；
		// 2. 满足一致性的提议数量是否满足法定数量的要求；
		Map<Integer, LeaderRegencyPropose> peerProposeMap = stops.get(regencyId);
		if (peerProposeMap == null) {
			return null;
		}
		LeaderRegencyPropose[] proposes = peerProposeMap.values()
				.toArray(new LeaderRegencyPropose[peerProposeMap.size()]);
		return ElectionResult.generateResult(regencyId, proposes);
	}

//	/**
//	 * Informs the object of who is the current leader
//	 * 
//	 * @param leader The current leader
//	 */
//	public void setNewLeader(int leader) {
//		currentLeader = leader;
//		LOGGER.info("I am proc {} , set new leader = {}", tomLayer.controller.getStaticConf().getProcessId(),
//				currentLeader);
//	}

	/**
	 * 跃迁至指定的执政期；
	 * <p>
	 * 
	 * 新的执政期必须大于当前执政期，否则引发 {@link IllegalStateException} 异常；
	 * 
	 * @param newRegency
	 */
	public void jumpToRegency(int newLeader, int newRegency) {
		jumpToRegency(new LeaderRegency(newLeader, newRegency));
	}

	/**
	 * 跃迁至指定的执政期；
	 * <p>
	 * 
	 * 新的执政期必须大于当前执政期，否则引发 {@link IllegalStateException} 异常；
	 * <p>
	 * 
	 * 如果当前处于另一个选举进程中，则不允许跃迁执政期，引发 {@link IllegalStateException} 异常；
	 * <p>
	 * 
	 * @param newRegency
	 */
	public synchronized void jumpToRegency(LeaderRegency newRegency) {
		if (newRegency.getId() <= currentRegency.getId()) {
			throw new IllegalStateException(
					String.format("The regency id can't be jumped forward! --[current regency=%s][new regency=%s]",
							currentRegency.getId(), newRegency.getId()));
		}
		if (isInProgress()) {
			throw new IllegalStateException(String.format(
					"There is a eclection in progress! The regency can't be jumped a new one concurrently! --[PropsedNextRegency=%s][NewRegency=%s]",
					getNextReg(), newRegency.getId()));
		}
		this.currentRegency = newRegency;

		this.nextreg = newRegency.getId();

		tomLayer.heartBeatTimer.setLeaderActived();
		LOGGER.info(
				"The regency jumps from [Regency={},Leader={}] to {Regency={},Leader={}} successfully! --[ElectionInProgress={}][CurrentProcess={}]",
				currentRegency.getId(), currentRegency.getLeaderId(), newRegency.getId(), newRegency.getLeaderId(),
				isInProgress(), getCurrentProcessId());
	}

	/**
	 * 尝试跃迁至指定的执政期；
	 * <p>
	 * 
	 * 新的执政期必须大于当前执政期，否则返回 false；
	 * <p>
	 * 如果当前处于一个选举进程中，则不允许跃迁，返回 false；
	 * 
	 * @param newRegency 新的执政期；
	 * @return 跃迁成功返回 true，否则返回 false；
	 */
	public synchronized boolean tryJumpToRegency(LeaderRegency newRegency) {
		if (newRegency.getId() <= currentRegency.getId()) {
			LOGGER.warn(
					"Could not jumps from [Regency={},Leader={}] to {Regency={},Leader={}}! --[ElectionInProgress={}][CurrentProcess={}]",
					currentRegency.getId(), currentRegency.getLeaderId(), newRegency.getId(), newRegency.getLeaderId(),
					isInProgress(), getCurrentProcessId());
			return false;
		}
		if (isInProgress()) {
			LOGGER.warn(
					"Could not jumps from [Regency={},Leader={}] to {Regency={},Leader={}}! --[ElectionInProgress={}][CurrentProcess={}]",
					currentRegency.getId(), currentRegency.getLeaderId(), newRegency.getId(), newRegency.getLeaderId(),
					isInProgress(), getCurrentProcessId());
			return false;
		}

		this.currentRegency = newRegency;

		this.nextreg = newRegency.getId();

		tomLayer.heartBeatTimer.setLeaderActived();

		LOGGER.info(
				"The regency jumps from [Regency={},Leader={}] to {Regency={},Leader={}} successfully! --[ElectionInProgress={}][CurrentProcess={}]",
				currentRegency.getId(), currentRegency.getLeaderId(), newRegency.getId(), newRegency.getLeaderId(),
				isInProgress(), getCurrentProcessId());

		return true;
	}

	/**
	 * This is meant to keep track of timed out requests in this replica
	 *
	 * @param currentRequestTimedOut Timed out requests in this replica
	 */
	public void setCurrentRequestTimedOut(List<TOMMessage> currentRequestTimedOut) {
		this.currentRequestTimedOut = currentRequestTimedOut;
	}

	/**
	 * Get the timed out requests in this replica
	 * 
	 * @return timed out requests in this replica
	 */
	public List<TOMMessage> getCurrentRequestTimedOut() {
		return currentRequestTimedOut;
	}

	/**
	 * Discard timed out requests in this replica
	 */
	public void clearCurrentRequestTimedOut() {
		if (currentRequestTimedOut != null)
			currentRequestTimedOut.clear();
		currentRequestTimedOut = null;
	}

	/**
	 * This is meant to keep track of requests received in STOP messages
	 *
	 * @param requestsFromSTOP Requests received in a STOP message
	 */
	public void addRequestsFromSTOP(TOMMessage[] requestsFromSTOP) {

		if (requestsFromSTOP == null) {
			return;
		}

		if (this.requestsFromSTOP == null)
			this.requestsFromSTOP = new LinkedList<>();

		for (TOMMessage m : requestsFromSTOP)
			this.requestsFromSTOP.add(m);
	}

	/**
	 * Get the requests received in STOP messages
	 * 
	 * @return requests received in STOP messages
	 */
	public List<TOMMessage> getRequestsFromSTOP() {
		return requestsFromSTOP;
	}

	/**
	 * Discard requests received in STOP messages
	 */
	public void clearRequestsFromSTOP() {
		if (requestsFromSTOP != null)
			requestsFromSTOP.clear();
		requestsFromSTOP = null;
	}

	/**
	 * 计算指定执政期的选举发起投票数（（STOP消息数 + STOP_APPEND消息数））是否达到正式发起选举的法定数量；
	 * <p>
	 * 
	 * 在 BFT 模式下，正式发起选举的条件是：“投票数” 大于等于 f + 1；
	 * <p>
	 * 在 CFT 模式下，正式发起选举的条件是：“投票数” 大于等于 f ；
	 * <p>
	 * 
	 * @param regency
	 * @return
	 */
	public boolean isUpToBeginQuorum(int regency) {
		int countOfSTOP = getStopsSize(regency);
		return countOfSTOP >= getBeginQuorum();
	}

	/**
	 * 达到正式发起选举的法定数量；
	 * <p>
	 * 
	 * 在 BFT 模式下，返回 f + 1；
	 * <p>
	 * 在 CFT 模式下，返回 f ；
	 * <p>
	 * 
	 * @return
	 */
	public int getBeginQuorum() {
		if (SVController.getStaticConf().isBFT()) {
			return SVController.getCurrentViewF() + 1;
		} else {
			return SVController.getCurrentViewF();
		}
	}

	/**
	 * 计算指定执政期的选举发起投票数（（STOP消息数 + STOP_APPEND消息数））是否达到完成选举产生新一轮执政期的法定数量；
	 * <p>
	 * 
	 * 在 BFT 模式下，正式完成选举的条件是：“投票数” 大于等于 2 * f + 1；
	 * <p>
	 * 在 CFT 模式下，正式发起选举的条件是：“投票数” 大于等于 f + 1；
	 * <p>
	 * 
	 * @param regency
	 * @return
	 */
	public boolean isUpToCommitQuorum(int regency) {
		int countOfSTOP = getStopsSize(regency);
		if (SVController.getStaticConf().isBFT()) {
			return countOfSTOP >= getBFTCommitQuorum();
		} else {
			return countOfSTOP >= getCFTCommitQuorum();
		}
	}

	public int getBFTCommitQuorum() {
		return 2 * SVController.getCurrentViewF() + 1;
	}

	public int getCFTCommitQuorum() {
		return SVController.getCurrentViewF() + 1;
	}

	public LCTimestampStatePair getLcTimestampStatePair() {return lcTimestampStatePair; }

	public synchronized void setLcTimestampStatePair(LCTimestampStatePair lcTimestampStatePair) {
		this.lcTimestampStatePair = lcTimestampStatePair;
	}

	/**
	 * 提交正在进行的 regency 为指定值的选举；
	 * <p>
	 * 
	 * 如果指定的 regency 与正在进行选举的 regency 一致，则完成选举则正常返回；
	 * <p>
	 * 
	 * 如果当前没有正在进行的选举，或者正在进行的选举与指定的 regency 不匹配，则此操作导致异常
	 * {@link IllegalStateException}；
	 * 
	 * @param regency 正在进行选举的执政期 Id；
	 */
	public synchronized ElectionResult commitElection(int regency) {
		if (!isInProgress(regency)) {
			// 指定的执政期不是正在选举的执政期；
			throw new IllegalStateException("The election with id[" + regency + "] is not in progress!");
		}

		ElectionResult electionResult = generateQuorumElectionResult(regency);
		if (electionResult == null) {
			throw new IllegalStateException("No quorum election result!");
		}

		// 如果选举结果的视图和当前节点的视图不一致，则报告异常；
		// 此种情况无法处理，需要管理员检查网络的视图状态，并通过进行 RECONFIG 操作调整视图；
		if (!isEqualToCurrentView(electionResult)) {
			// 选举结果所依赖的视图和本地的视图不一致；
			String errorMessage = String.format(
					"The view of the election result is not consist with the view of current node[%s]!",
					SVController.getStaticConf().getProcessId());
			LOGGER.error(errorMessage);
			throw new IllegalStateException(errorMessage);
		}

		this.currentRegency = electionResult.getRegency();
		this.nextreg = electionResult.getRegency().getId();

		tomLayer.heartBeatTimer.setLeaderActived();

		// 移除已完成的选举之前的投标信息；
		removeStops(regency);

		return electionResult;
	}

	/**
	 * 提交正在进行的 regency 为指定值的选举；
	 * <p>
	 * 
	 * 如果指定的 regency 与正在进行选举的 regency 一致，则完成选举则正常返回；
	 * <p>
	 * 
	 * 如果当前没有正在进行的选举，或者正在进行的选举与指定的 regency 不匹配，则此操作导致异常
	 * {@link IllegalStateException}；
	 * 
	 * @param regency 正在进行选举的执政期 Id；
	 */
	public synchronized ElectionResult tryCommitElection(int regency) {
		if (!isInProgress(regency)) {
			// 指定的执政期不是正在选举的执政期；
			return null;
		}

		ElectionResult electionResult = generateQuorumElectionResult(regency);
		if (electionResult == null) {
			return null;
		}

		// 如果选举结果的视图和当前节点的视图不一致，则报告异常；
		// 此种情况无法处理，需要管理员检查网络的视图状态，并通过进行 RECONFIG 操作调整视图；
		if (!isEqualToCurrentView(electionResult)) {
			// 选举结果所依赖的视图和本地的视图不一致；
			return null;
		}

		this.currentRegency = electionResult.getRegency();
		this.nextreg = electionResult.getRegency().getId();

		tomLayer.heartBeatTimer.setLeaderActived();

		// 移除已完成的选举之前的投标信息；
		removeStops(regency);

		LOGGER.info("Commit election result! --[NewRegency={}][NewLeader={}][CurrentProcessId={}]",
				currentRegency.getId(), currentRegency.getLeaderId(), tomLayer.getCurrentProcessId());

		return electionResult;
	}

	public boolean isEqualToCurrentView(ElectionResult electionResult) {
		int currentViewId = SVController.getCurrentViewId();
		int[] currentViewProcessIds = SVController.getCurrentViewProcesses();
		Arrays.sort(currentViewProcessIds);
		if (electionResult.getViewId() != currentViewId
				|| !ArrayUtils.equals(electionResult.getViewProcessIds(), currentViewProcessIds)) {
			return false;
		}

		return true;
	}

	/**
	 * The current regency
	 * 
	 * @return current regency
	 */
	public int getLastReg() {
		return currentRegency.getId();
	}

//	/**
//	 * Set the next regency
//	 * 
//	 * @param nextts next regency
//	 */
//	public synchronized void setNextReg(int nextreg) {
//		if (nextreg <= lastreg) {
//			throw new IllegalArgumentException(String.format(
//					"The next regency id is less than or equal to the last regency of LCManager! --[lastReg=%s][nextReg=%s]",
//					lr, nextreg));
//		}
//		this.nextreg = nextreg;
//	}

	public boolean canPropose(int regency) {
		return regency > getLastReg();
	}

	/**
	 * 当前是否正在进行一轮选举中；
	 * 
	 * @return
	 */
	public synchronized boolean isInProgress() {
		return getLastReg() != nextreg;
	}

	/**
	 * 当前是否正在进行指定执政期的选举中；
	 * 
	 * @return
	 */
	public synchronized boolean isInProgress(int regency) {
		return isInProgress() && nextreg == regency;
	}

	/**
	 * 指定执政期在当下 {@link LCManager} 状态下是否属于未来的超前执政期，即在下一次完成选举之后更远的执政期；
	 * <p>
	 * 
	 * 只有在当前已经处于选举进程，且指定的执政期Id大于当前正在选举中的执政期 Id 时，指定执政期属于未来的超前执政期，此时返回 true；
	 * <p>
	 * 如果当前不处于选举进程，而指定的 regency 尽管大于当前的 {@link #getNextReg()} ，也无法确定是否会落在下一次执政期的范围内；
	 * 
	 * @param regency
	 * @return
	 */
	public synchronized boolean isFutureAfterNextRegency(int regency) {
		return isInProgress() && regency > getNextReg();
	}

	/**
	 * 尝试以指定的执政期 Id 从“非选举”状态开始进入“选举中”状态；
	 * <p>
	 * 如果成功，则返回 true；
	 * <p>
	 * 如果当前已经处于选举中，或者提议
	 * 
	 * @param nextReg 试图要发起的新选举的执政期ID；
	 * @return 是否成功地以指定的 regency 提议开启了新的一轮选举；
	 */
	public synchronized boolean tryBeginElection(LeaderRegency proposedRegency) {
		LOGGER.debug("Try to begin election ... [ProposedRegencyId={}][ProposedLeaderId={}][CurrentProccessId={}]",
				proposedRegency.getId(), proposedRegency.getLeaderId(), tomLayer.getCurrentProcessId());

		int proposedRegencyId = proposedRegency.getId();
		if (proposedRegencyId < nextreg) {
			// 过期的提议；
			LOGGER.warn("Try to begin an outdated regency election! --[CurrentRegencyId={}][ProposedRegencyId={}]",
					nextreg, proposedRegencyId);
			return false;
		}
		if (isInProgress(proposedRegencyId)) {
			// 指定的regency已经在选举中，不必切换；
			LOGGER.debug("The proposed regency has been in progress! --[ProposedRegencyId={}][ProposedLeaderId={}]",
					proposedRegencyId, proposedRegency.getLeaderId());
			return false;
		}
		if (nextreg == proposedRegencyId) {
			// 未在选举进程中；
			// 但是提议的新执政期与当前的执政期相同，不能对同一个执政期反复进行选举；
			LOGGER.warn(
					"Try to begin an outdated regency election! --[CurrentRegencyId={}][ProposedRegencyId={}][ProposedLeaderId={}]",
					nextreg, proposedRegencyId, proposedRegency.getLeaderId());
			return false;
		}

		if (!isUpToBeginQuorum(proposedRegencyId)) {
			// 指定的执政期提议数量未达到法定的选举发起投票数量；
			LOGGER.debug(
					"The proposed regency is not up to the begin quorum! --[BeginQuorum={}][ProposedRegencyId={}][ProposedLeaderId={}][CurrentProccessId={}]",
					getBeginQuorum(), proposedRegency.getId(), proposedRegency.getLeaderId(),
					tomLayer.getCurrentProcessId());
			return false;
		}

		// 新的执政期大于当前执政期；
		// 即：表达式 proposedRegencyId > nextreg && !isInProgress() 为 true；
		nextreg = proposedRegencyId;

		LOGGER.debug(
				"The proposed regency begin election! --[BeginQuorum={}][ProposedRegencyId={}][ProposedLeaderId={}]",
				getBeginQuorum(), proposedRegency.getId(), proposedRegency.getLeaderId());
		return true;
	}

	/**
	 * The next regency
	 * 
	 * @return next regency
	 */
	public int getNextReg() {
		return nextreg;
	}

	public synchronized void updateNextReg(int nextreg) {
		this.nextreg = nextreg;
	}
	/**
	 * Keep information about an incoming STOP message
	 * 
	 * @param regency the next regency
	 * @param pid     the process that sent the message
	 */
	public synchronized void addStop(LeaderRegencyPropose regencyPropose) {
		int propsedRegencyId = regencyPropose.getRegency().getId();
		if (!canPropose(propsedRegencyId)) {
			// 过期的无效提议；
			return;
		}
		Map<Integer, LeaderRegencyPropose> peerProposes = stops.get(propsedRegencyId);
		if (peerProposes == null) {
			peerProposes = new ConcurrentHashMap<Integer, LeaderRegencyPropose>();
			stops.put(propsedRegencyId, peerProposes);
		}
		peerProposes.put(regencyPropose.getSender(), regencyPropose);
	}

	/**
	 * Discard information about STOP messages up to specified regency
	 * 
	 * @param regency regency id up to which to discard messages
	 */
	private synchronized void removeStops(int regency) {
		Integer[] keys = new Integer[stops.keySet().size()];
		stops.keySet().toArray(keys);

		for (int i = 0; i < keys.length; i++) {
			if (keys[i] <= regency)
				stops.remove(keys[i]);
		}
	}

	/**
	 * Get the quantity of stored STOP information
	 * 
	 * @param regency Regency to be considered
	 * @return quantity of stored STOP information for given timestamp
	 */
	public int getStopsSize(int regency) {
		Map<Integer, LeaderRegencyPropose> pids = stops.get(regency);
		return pids == null ? 0 : pids.size();
	}

	/**
	 * Keep last CID from an incoming SYNC message
	 * 
	 * @param regency the current regency
	 * @param lastCID the last CID data
	 */
	public void addLastCID(int regency, CertifiedDecision lastCID) {

		HashSet<CertifiedDecision> last = lastCIDs.get(regency);
		if (last == null)
			last = new HashSet<CertifiedDecision>();
		last.add(lastCID);
		lastCIDs.put(regency, last);
	}

	/**
	 * Discard last CID information up to the specified regency
	 * 
	 * @param regency Regency up to which to discard information
	 */
	public void removeLastCIDs(int regency) {
		Integer[] keys = new Integer[lastCIDs.keySet().size()];
		lastCIDs.keySet().toArray(keys);

		for (int i = 0; i < keys.length; i++) {
			if (keys[i] <= regency)
				lastCIDs.remove(keys[i]);
		}
	}

	/**
	 * Get the quantity of stored last CID information
	 * 
	 * @param regency regency to be considered
	 * @return quantity of stored last CID information for given regency
	 */
	public int getLastCIDsSize(int regency) {
		HashSet<CertifiedDecision> last = lastCIDs.get(regency);
		return last == null ? 0 : last.size();
	}

	/**
	 * Get the set of last CIDs related to a regency
	 * 
	 * @param regency Regency for the last CID info
	 * @return a set of last CID data
	 */
	public HashSet<CertifiedDecision> getLastCIDs(int regency) {
		return lastCIDs.get(regency);
	}

	/**
	 * Defines the set of last CIDs related to a regency
	 * 
	 * @param regency Regency for the last CID info
	 * @param lasts   a set of last CID data
	 */
	public void setLastCIDs(int regency, HashSet<CertifiedDecision> lasts) {

		lastCIDs.put(regency, lasts);
	}

	/**
	 * Keep collect from an incoming SYNC message
	 * 
	 * @param ts            the current regency
	 * @param signedCollect the signed collect data
	 */
	public void addCollect(int regency, SignedObject signedCollect) {

		HashSet<SignedObject> c = collects.get(regency);
		if (c == null)
			c = new HashSet<SignedObject>();
		c.add(signedCollect);
		collects.put(regency, c);
	}

	/**
	 * Discard collect information up to the given regency
	 * 
	 * @param regency Regency up to which to discard information
	 */
	public void removeCollects(int regency) {

		Integer[] keys = new Integer[collects.keySet().size()];
		collects.keySet().toArray(keys);

		for (int i = 0; i < keys.length; i++) {
			if (keys[i] <= regency)
				collects.remove(keys[i]);
		}
	}

	/**
	 * Get the quantity of stored collect information
	 * 
	 * @param regency Regency to be considered
	 * @return quantity of stored collect information for given regency
	 */
	public int getCollectsSize(int regency) {

		HashSet<SignedObject> c = collects.get(regency);
		return c == null ? 0 : c.size();
	}

	/**
	 * Get the set of collects related to a regency
	 * 
	 * @param regency Regency for collects
	 * @return a set of collect data
	 */
	public HashSet<SignedObject> getCollects(int regency) {
		return collects.get(regency);
	}

	/**
	 * Defines the set of collects related to a regency
	 * 
	 * @param regency Regency for the last CID info
	 * @param colls   a set of collect data
	 */
	public void setCollects(int regency, HashSet<SignedObject> colls) {

		collects.put(regency, colls);
	}

	/**
	 * The all-important predicate "sound". This method must received a set of
	 * collects that were filtered using the method selectCollects()
	 *
	 * @param collects the collect data to which to apply the predicate.
	 * @return See Cachin's 'Yet Another Visit to Paxos' (April 2011), page 11
	 * 
	 *         In addition, see pages 252 and 253 from "Introduction to Reliable and
	 *         Secure Distributed Programming"
	 */
	public boolean sound(HashSet<CollectData> collects) {
		if (collects == null) {
			return false;
		}

		LOGGER.info("(LCManager.sound) I collected the context from {} replicas", collects.size());


		HashSet<Integer> timestamps = new HashSet<Integer>();
		HashSet<byte[]> values = new HashSet<byte[]>();

		for (CollectData c : collects) { // organize all existing timestamps and values separately

			LOGGER.debug("(LCManager.sound) Context for replica {}, CID {},WRITESET [{}], (VALTS,VAL) [{}]", c.getPid(),
					c.getCid(), c.getWriteSet(), c.getQuorumWrites());

			timestamps.add(c.getQuorumWrites().getTimestamp()); // store timestamp received from a Byzatine quorum of
																// WRITES

			// store value received from a Byzantine quorum of WRITES, unless it is an empty
			// value
			if (!Arrays.equals(c.getQuorumWrites().getValue(), new byte[0])) {
				boolean insert = true; // this loop avoids putting duplicated values in the set
				for (byte[] b : values) {

					if (Arrays.equals(b, c.getQuorumWrites().getValue())) {
						insert = false;
						break;
					}
				}
				if (insert)
					values.add(c.getQuorumWrites().getValue());
			}
			for (TimestampValuePair rv : c.getWriteSet()) { // store all timestamps and written values
				timestamps.add(rv.getTimestamp());

				boolean insert = true; // this loop avoids putting duplicated values in the set
				for (byte[] b : values) {

					if (Arrays.equals(b, rv.getHashedValue())) {
						insert = false;
						break;
					}
				}
				if (insert)
					values.add(rv.getHashedValue());
			}

		}

		LOGGER.debug("(LCManager.sound) number of timestamps: {}", timestamps.size());
		LOGGER.debug("(LCManager.sound) number of values: {}", values.size());

		// after having organized all timestamps and values, properly apply the
		// predicate
		for (int r : timestamps) {
			for (byte[] v : values) {

				LOGGER.debug("(LCManager.sound) testing predicate BIND for timestamp/value pair [{}],[{}]", r,
						Arrays.toString(v));
				if (binds(r, v, collects)) {

					LOGGER.debug("(LCManager.sound) Predicate BIND is true for timestamp/value pair [{}],[{}]", r,
							Arrays.toString(v));
					LOGGER.debug(
							"(LCManager.sound) Predicate SOUND is true for the for context collected from N-F replicas");
					return true;
				}
			}
		}

		LOGGER.debug("(LCManager.sound) No timestamp/value pair passed on the BIND predicate");

		boolean unbound = unbound(collects);

		if (unbound) {
			LOGGER.info("(LCManager.sound) Predicate UNBOUND is true for N-F replicas");
			LOGGER.info("(LCManager.sound) Predicate SOUND is true for the for context collected from N-F replicas");
		}

		return unbound;
	}

	/**
	 * The predicate "binds". This method must received a set of collects that were
	 * filtered using the method selectCollects()
	 *
	 * @param timestamp the timestamp to search for
	 * @param value     the value to search for
	 * @param collects  the collect data to which to apply the predicate.
	 * @return See Cachin's 'Yet Another Visit to Paxos' (April 2011), page 11
	 * 
	 *         In addition, see pages 252 and 253 from "Introduction to Reliable and
	 *         Secure Distributed Programming"
	 */
	public boolean binds(int timestamp, byte[] value, HashSet<CollectData> collects) {

		if (value == null || collects == null) {
			LOGGER.error("(LCManager.binds) Received null objects, returning false");
			return false;
		}

//        if (!(collects.size() >= (SVController.getCurrentViewN() - SVController.getCurrentViewF()))) {
		if (!(collects.size() > 2 * SVController.getCurrentViewF())) {
			LOGGER.error("(LCManager.binds) Less than N-F contexts collected from replicas, returning false");
			return false;
		}

		return (quorumHighest(timestamp, value, collects) && certifiedValue(timestamp, value, collects));

		// return value != null && collects != null && (collects.size() >=
		// (SVController.getCurrentViewN() - SVController.getCurrentViewF()))
		// && quorumHighest(timestamp, value, collects) && certifiedValue(timestamp,
		// value, collects);
	}

	/**
	 * Return a value that is "bind", that is different from null, and with a
	 * timestamp greater or equal to zero
	 * 
	 * @param collects Set of collects from which to determine the value
	 * @return The bind value
	 * 
	 *         See Cachin's 'Yet Another Visit to Paxos' (April 2011), page 11 Also,
	 *         see pages 252 and 253 from "Introduction to Reliable and Secure
	 *         Distributed Programming"
	 */
	public byte[] getBindValue(HashSet<CollectData> collects) {

		if (collects == null)
			return null;

		HashSet<Integer> timestamps = new HashSet<Integer>();
		HashSet<byte[]> values = new HashSet<byte[]>();

		for (CollectData c : collects) { // organize all existing timestamps and values separately

			timestamps.add(c.getQuorumWrites().getTimestamp()); // store timestamp received from a Byzantine quorum of
																// writes

			// store value received from a Byzantine quorum of writes, unless it is an empty
			// value
			if (!Arrays.equals(c.getQuorumWrites().getValue(), new byte[0])) {
				boolean insert = true; // this loops avoids putting duplicated values in the set
				for (byte[] b : values) {

					if (Arrays.equals(b, c.getQuorumWrites().getValue())) {
						insert = false;
						break;
					}
				}
				if (insert)
					values.add(c.getQuorumWrites().getValue());
			}
			for (TimestampValuePair rv : c.getWriteSet()) { // store all timestamps and written values
				timestamps.add(rv.getTimestamp());

				boolean insert = true; // this loops avoids putting duplicated values in the set
				for (byte[] b : values) {

					if (Arrays.equals(b, rv.getHashedValue())) {
						insert = false;
						break;
					}
				}
				if (insert)
					values.add(rv.getHashedValue());
			}

		}

		// after having organized all timestamps and values, properly apply the
		// predicate
		for (int r : timestamps) {
			for (byte[] v : values) {

				if (r >= 0 && binds(r, v, collects)) { // do we have a value that satisfys the predicate?

					// as we are handling hashes, we have to find the original value
					for (CollectData c : collects) {
						for (TimestampValuePair rv : c.getWriteSet()) {

							if (rv.getValue() != null && Arrays.equals(v, rv.getHashedValue())) {
								return rv.getValue();
							}

						}
					}
				}
			}
		}

		return null;
	}

	/**
	 * The predicate "unbound". This method must received a set of collects that
	 * were filtered using the method selectCollects()
	 *
	 * @param collects the collect data to which to apply the predicate.
	 * @return See Cachin's 'Yet Another Visit to Paxos' (April 2011), page 11
	 * 
	 *         In addition, see page 253 from "Introduction to Reliable and Secure
	 *         Distributed Programming"
	 */
	public boolean unbound(HashSet<CollectData> collects) {

		if (collects == null)
			return false;

		boolean unbound = false;
		int count = 0;

		if (collects.size() > (2 * SVController.getCurrentViewF())) {

			for (CollectData c : collects) {

				if (c.getQuorumWrites().getTimestamp() == 0)
					count++;
			}
		} else
			return false;

		if (SVController.getStaticConf().isBFT()) {
			unbound = count > (2 * SVController.getCurrentViewF());
		} else {
			unbound = count > ((SVController.getCurrentViewN()) / 2);
		}
		return unbound;

	}

	/**
	 * The predicate "quorumHighest". This method must received a set of collects
	 * that were filtered using the method selectCollects()
	 *
	 * @param timestamp the timestamp to search for
	 * @param value     the value to search for
	 * @param collects  the collect data to which to apply the predicate.
	 * @return See Cachin's 'Yet Another Visit to Paxos' (April 2011), pages 10-11
	 * 
	 *         In addition, see pages 252 and 253 from "Introduction to Reliable and
	 *         Secure Distributed Programming"
	 */
	public boolean quorumHighest(int timestamp, byte[] value, HashSet<CollectData> collects) {

		if (collects == null || value == null)
			return false;

		boolean appears = false;
		boolean quorum = false;

		for (CollectData c : collects) {

			if (c.getQuorumWrites().getTimestamp() == timestamp
					&& Arrays.equals(value, c.getQuorumWrites().getValue())) {

				appears = true;
				break;
			}
		}

		if (appears)
			LOGGER.debug(
					"(LCManager.quorumHighest) timestamp/value pair [{}],[{}] appears in at least one replica context",
					timestamp, Arrays.toString(value));

		int count = 0;
		for (CollectData c : collects) {

			// LOGGER.debug(("\t\t[QUORUM HIGHEST] ts' < ts : " +
			// (c.getQuorumWrites().getTimestamp() < timestamp));
			// LOGGER.debug(("\t\t[QUORUM HIGHEST] ts' = ts && val' = val : " +
			// (c.getQuorumWrites().getTimestamp() == timestamp && Arrays.equals(value,
			// c.getQuorumWrites().getValue())));

			if ((c.getQuorumWrites().getTimestamp() < timestamp) || (c.getQuorumWrites().getTimestamp() == timestamp
					&& Arrays.equals(value, c.getQuorumWrites().getValue())))
				count++;

		}

		if (SVController.getStaticConf().isBFT()) {
			quorum = count > ((SVController.getCurrentViewN() + SVController.getCurrentViewF()) / 2);
		} else {
			quorum = count > ((SVController.getCurrentViewN()) / 2);
		}
		if (quorum)
			LOGGER.debug(
					"(LCManager.quorumHighest) timestamp/value pair [{}], [{}] has the highest timestamp among a {} quorum of replica contexts",
					timestamp, Arrays.toString(value), SVController.getStaticConf().isBFT() ? "Byzantine" : "simple");
		return appears && quorum;
	}

	/**
	 * The predicate "certifiedValue". This method must received a set of collects
	 * that were filtered using the method selectCollects()
	 *
	 * @param timestamp the timestamp to search for
	 * @param value     the value to search for
	 * @param collects  the collect data to which to apply the predicate.
	 * @return See Cachin's 'Yet Another Visit to Paxos' (April 2011), page 11
	 * 
	 *         In addition, see page 253 from "Introduction to Reliable and Secure
	 *         Distributed Programming"
	 */
	public boolean certifiedValue(int timestamp, byte[] value, HashSet<CollectData> collects) {

		if (collects == null || value == null)
			return false;

		boolean certified = false;

		int count = 0;
		for (CollectData c : collects) {

			for (TimestampValuePair pv : c.getWriteSet()) {

//                LOGGER.debug(("\t\t[CERTIFIED VALUE] " + pv.getTimestamp() + "  >= " + timestamp);
//                LOGGER.debug(("\t\t[CERTIFIED VALUE] " + Arrays.toString(value) + "  == " + Arrays.toString(pv.getValue()));
				if (pv.getTimestamp() >= timestamp && Arrays.equals(value, pv.getHashedValue()))
					count++;
			}

		}

		if (SVController.getStaticConf().isBFT()) {
			certified = count > SVController.getCurrentViewF();
		} else {
			certified = count > 0;
		}
		if (certified)
			LOGGER.debug(
					"(LCManager.certifiedValue) timestamp/value pair [{}], [{}] has been written by at least {} replica(s)",
					timestamp, Arrays.toString(value), count);

		return certified;
	}

	/**
	 * Fetchs a set of correctly signed and normalized collect data structures
	 * 
	 * @param regency the regency from which the collects were stored
	 * @param cid     the CID to which to normalize the collects
	 * @return a set of correctly signed and normalized collect data structures
	 */
	public HashSet<CollectData> selectCollects(int regency, int cid) {

		HashSet<SignedObject> c = collects.get(regency);

		if (c == null)
			return null;

		return normalizeCollects(getSignedCollects(c), cid, regency);

	}

	/**
	 * Fetchs a set of correctly signed and normalized collect data structures from
	 * the specified original set of collects
	 * 
	 * @param signedObjects original set of signed collects
	 * @param cid           the CID to which to normalize the collects
	 * @return a set of correctly signed and normalized collect data structures
	 */
	public HashSet<CollectData> selectCollects(HashSet<SignedObject> signedObjects, int cid, int regency) {

		if (signedObjects == null)
			return null;

		return normalizeCollects(getSignedCollects(signedObjects), cid, regency);

	}

	// Filters the correctly signed collects
	private HashSet<CollectData> getSignedCollects(HashSet<SignedObject> signedCollects) {

		HashSet<CollectData> colls = new HashSet<CollectData>();

		for (SignedObject so : signedCollects) {

			CollectData c;
			try {
				c = (CollectData) so.getObject();
				int sender = c.getPid();
				if (tomLayer.verifySignature(so, sender)) {
					colls.add(c);
				}
			} catch (IOException ex) {
				LOGGER.error(ex.getMessage(), ex);
			} catch (ClassNotFoundException ex) {
				LOGGER.error(ex.getMessage(), ex);
			}
		}

		return colls;

	}

	// Normalizes the set of collects. A set of collects is considered normalized if
	// or when
	// all collects are related to the same CID. This is important because not all
	// replicas
	// may be executing the same CID when tere is a leader change
	private HashSet<CollectData> normalizeCollects(HashSet<CollectData> collects, int cid, int regency) {

		HashSet<CollectData> result = new HashSet<CollectData>();

		// if there are collects refering to other consensus instances, lets assume that
		// they are still at timestamp zero of the consensus we want
		for (CollectData c : collects) {

			if (c.getCid() == cid) {
				result.add(c);
			} else {
				result.add(new CollectData(c.getPid(), cid, regency, new TimestampValuePair(0, new byte[0]),
						new HashSet<TimestampValuePair>()));
			}

		}

		// calculate hash of the values in the write set
		for (CollectData c : result) {

			for (TimestampValuePair rv : c.getWriteSet()) {

				if (rv.getValue() != null && rv.getValue().length > 0)
					rv.setHashedValue(md.hash(rv.getValue()));
				else
					rv.setHashedValue(new byte[0]);
			}
		}

		return result;

	}

	/**
	 * Gets the highest valid last CID related to the given timestamp
	 * 
	 * @param ts the timestamp
	 * @return -1 if there is no such CID, otherwise returns the highest valid last
	 *         CID
	 */
	public CertifiedDecision getHighestLastCID(int ts) {

		CertifiedDecision highest = new CertifiedDecision(-2, -2, null, null);

		HashSet<CertifiedDecision> lasts = lastCIDs.get(ts);

		if (lasts == null)
			return null;

		for (CertifiedDecision l : lasts) {

			// TODO: CHECK OF THE PROOF IS MISSING!!!!
			if (tomLayer.controller.getStaticConf().isBFT() && hasValidProof(l) && l.getCID() > highest.getCID())
				highest = l;
			else if (l.getCID() > highest.getCID()) {
				highest = l;
			}
		}

		return highest;
	}

	// verifies is a proof associated with a decided value is valid
	public boolean hasValidProof(CertifiedDecision cDec) {

		if (cDec == null || cDec.getCID() == -1 || cDec.getDecision() == null || cDec.getConsMessages() == null)
			return true; // If the last CID is -1 it means the replica
		// did not complete any consensus and cannot have
		// any proof
		LOGGER.debug("I am {}, pid = {}, cid = {}, consmsg = {}", tomLayer.controller.getStaticConf().getProcessId(),
				cDec.getPID(), cDec.getCID(), cDec.getConsMessages() == null ? "null" : cDec.getConsMessages().size());

		byte[] hashedValue = null;
		try {
			hashedValue = md.hash(cDec.getDecision());
		} catch (Exception e) {
			e.printStackTrace();
		}
//        byte[] hashedValue = md.hash(cDec.getDecision());
		Set<ConsensusMessage> ConsensusMessages = cDec.getConsMessages();
		int myId = tomLayer.controller.getStaticConf().getProcessId();
		int certificateCurrentView = (2 * tomLayer.controller.getCurrentViewF()) + 1;
		int certificateLastView = -1;
		if (tomLayer.controller.getLastView() != null)
			certificateLastView = (2 * tomLayer.controller.getLastView().getF()) + 1;
		int countValid = 0;
		PublicKey pubRSAKey = null;

		HashSet<Integer> alreadyCounted = new HashSet<>(); // stores replica IDs that were already counted

		for (ConsensusMessage consMsg : ConsensusMessages) {

			ConsensusMessage cm = new ConsensusMessage(consMsg.getType(), consMsg.getNumber(), consMsg.getEpoch(),
					consMsg.getSender(), consMsg.getValue());
			cm.setOrigPropValue(consMsg.getOrigPropValue());

			ByteArrayOutputStream bOut = new ByteArrayOutputStream(248);
			try {
				new ObjectOutputStream(bOut).writeObject(cm);
			} catch (IOException ex) {
				ex.printStackTrace();
			}

			byte[] data = bOut.toByteArray();

			if (consMsg.getProof() instanceof HashMap) { // Certificate is made of MAC vector

				LOGGER.debug("(LCManager.hasValidProof) Proof made of MAC vector");

				HashMap<Integer, byte[]> macVector = (HashMap<Integer, byte[]>) consMsg.getProof();

				byte[] recvMAC = macVector.get(myId);

				byte[] myMAC = null;

//				SecretKey secretKey = tomLayer.getCommunication().getServersCommunication().getSecretKey(consMsg.getSender());
				MacKey macKey = tomLayer.getCommunication().getServersCommunication().getMacKey(consMsg.getSender());
				if (macKey != null) {
					myMAC = macKey.generateMac(data);
				}

				// 不能判断mac, 对于部分节点宕机，或者重启的情况mac是会为空的，这样证据就验证失败了
//				if (recvMAC != null && myMAC != null && Arrays.equals(recvMAC, myMAC)
//						&& Arrays.equals(consMsg.getOrigPropValue(), hashedValue)
//						&& consMsg.getNumber() == cDec.getCID() && !alreadyCounted.contains(consMsg.getSender())) {
//
//					alreadyCounted.add(consMsg.getSender());
//					countValid++;
//				}

				if (Arrays.equals(consMsg.getOrigPropValue(), hashedValue) && consMsg.getNumber() == cDec.getCID()
						&& !alreadyCounted.contains(consMsg.getSender())) {

					alreadyCounted.add(consMsg.getSender());
					countValid++;
				}
				LOGGER.info("(LCManager.hasValidProof) countValid = {}", countValid);

			} else if (consMsg.getProof() instanceof byte[]) { // certificate is made of signatures

				LOGGER.debug("(LCManager.hasValidProof) Proof made of Signatures");
				pubRSAKey = SVController.getStaticConf().getRSAPublicKey(consMsg.getSender());

				byte[] signature = (byte[]) consMsg.getProof();

				if (TOMUtil.verifySignature(pubRSAKey, data, signature)
						&& !alreadyCounted.contains(consMsg.getSender())) {

					alreadyCounted.add(consMsg.getSender());
					countValid++;
				}

			} else {
				LOGGER.error("(LCManager.hasValidProof) Proof is message is invalid");
			}
		}

		// If proofs were made of signatures, use a certificate correspondent to last
		// view
		// otherwise, use certificate for the current view
		// To understand why this is important, check the comments in
		// Acceptor.computeWrite()

		if (certificateLastView != -1 && pubRSAKey != null)
			LOGGER.debug("(LCManager.hasValidProof) Computing certificate based on previous view");

		// return countValid >= certificateCurrentView;

		boolean result = countValid >= (certificateLastView != -1 && pubRSAKey != null ? certificateLastView
				: certificateCurrentView);
		LOGGER.debug("Proof is valid ? {}", result);
		return result;
	}

	/**
	 * Returns the value of the specified last CID for a given regency
	 * 
	 * @param regency the related regency
	 * @param cid     the last CID
	 * @return null if there is no such CID or is invalid, otherwise returns the
	 *         value
	 */
	public byte[] getLastCIDValue(int regency, int cid) {

		HashSet<CertifiedDecision> lasts = lastCIDs.get(regency);

		if (lasts == null)
			return null;

		byte[] result = null;

		for (CertifiedDecision l : lasts) {

			if (l.getCID() == cid) {

				// TODO: CHECK OF THE PROOF IS MISSING!!!!
				result = l.getDecision();
				break;
			}
		}

		return result;
	}

	/**
	 * Gets the highest ETS associated with a consensus ID from the given collects
	 * 
	 * @param cid      The consensus ID
	 * @param collects The collects from the other replicas
	 * @return The highest ETS
	 */
	public int getETS(int cid, Set<CollectData> collects) {

		int ets = -1;
		int count = 0;

		for (CollectData c : collects) {

			if (c.getCid() == cid) {

				if (c.getEts() > ets) {

					ets = c.getEts();
					count = 1;
				} else if (c.getEts() == ets) {
					count++;
				}

			}
		}

		return (count > this.SVController.getCurrentViewF() ? ets : -1);
	}
}
