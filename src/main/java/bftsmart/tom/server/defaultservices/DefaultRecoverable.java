/**
 * Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and
 * the authors indicated in the
 *
 * @author tags
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package bftsmart.tom.server.defaultservices;

import bftsmart.consensus.app.BatchAppResultImpl;
import bftsmart.consensus.app.PreComputeBatchExecutable;
import bftsmart.consensus.app.SHA256Utils;
import bftsmart.reconfiguration.ViewTopology;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.SMMessage;
import bftsmart.statemanagement.StateManager;
import bftsmart.statemanagement.strategy.BaseStateManager;
import bftsmart.statemanagement.strategy.StandardSMMessage;
import bftsmart.statemanagement.strategy.StandardStateManager;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ReplicaConfiguration;
import bftsmart.tom.ReplicaContext;
import bftsmart.tom.ReplyContextMessage;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.util.TOMUtil;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * This class provides a basic state transfer protocol using the interface
 * 'BatchExecutable'.
 * 
 * @author Joao Sousa
 */
public abstract class DefaultRecoverable implements Recoverable, PreComputeBatchExecutable {

	private int checkpointPeriod;
	private ReentrantLock logLock = new ReentrantLock();
	private ReentrantLock hashLock = new ReentrantLock();
	private ReentrantLock stateLock = new ReentrantLock();
	private ReplicaConfiguration config;
	private ViewTopology controller;
	private SHA256Utils md = new SHA256Utils();
	private StateLog log;
	private String realName;
	private StateManager stateManager;
	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(DefaultRecoverable.class);

	public DefaultRecoverable() {

//        try {
//            md = MessageDigest.getInstance("MD5"); // TODO: shouldn't it be SHA?
//        } catch (NoSuchAlgorithmException ex) {
//            java.util.logging.Logger.getLogger(DefaultRecoverable.class.getName()).log(Level.SEVERE, null, ex);
//        }
	}

	@Override
	public BatchAppResultImpl preComputeHash(int cid, byte[][] commands, long timestamp) {
		return preComputeAppHash(cid, commands, timestamp);
	}

	@Override
	public void preComputeCommit(int cid, String batchId) {
		preComputeAppCommit(cid, batchId);
	}

	@Override
	public void preComputeRollback(int cid, String batchId) {
		preComputeAppRollback(cid, batchId);
	}

	@Override
	public List<byte[]> updateResponses(List<byte[]> asyncResponseLinkedList, byte[] commonHash, boolean isConsistent) {
		return updateAppResponses(asyncResponseLinkedList, commonHash, isConsistent);
	}

//    @Override
//    public byte[][] executeBatch(byte[][] commands, MessageContext[] msgCtxs) {
//        return executeBatch(commands, msgCtxs, false, null);
//    }

	@Override
	public byte[][] executeBatch(byte[][] commands, MessageContext[] msgCtxs) {
		return executeBatch(commands, msgCtxs, false);
	}

	// 适用于commands中包含多轮共识批次消息的设计，需要时可用，目前设计中只包含一轮批次消息，故可以简化处理流程
//	private byte[][] executeBatch(byte[][] commands, MessageContext[] msgCtxs, boolean noop) {
//
//		int cid = msgCtxs[msgCtxs.length - 1].getConsensusId();
//
//		// As the delivery thread may deliver several consensus at once it is necessary
//		// to find if a checkpoint might be taken in the middle of the batch execution
//		int[] cids = consensusIds(msgCtxs);
//		int checkpointIndex = findCheckpointPosition(cids);
//
//		byte[][] replies = new byte[commands.length][];
//
//		if (checkpointIndex == -1) {
//
////			if (!noop) {
////
////				stateLock.lock();
////				if (replyContextMessages != null && !replyContextMessages.isEmpty()) {
////					replies = appExecuteBatch(commands, msgCtxs, true, replyContextMessages);
////				} else {
////					replies = appExecuteBatch(commands, msgCtxs, true);
////				}
////				stateLock.unlock();
////
////			}
//
//			saveCommands(commands, msgCtxs);
//		} else {
//			// there is a replica supposed to take the checkpoint. In this case, the
//			// commands
//			// must be executed in two steps. First the batch of commands containing
//			// commands
//			// until the checkpoint period is executed and the log saved or checkpoint taken
//			// if this replica is the one supposed to take the checkpoint. After the
//			// checkpoint
//			// or log, the pointer in the log is updated and then the remaining portion of
//			// the
//			// commands is executed
//			byte[][] firstHalf = new byte[checkpointIndex + 1][];
//			MessageContext[] firstHalfMsgCtx = new MessageContext[firstHalf.length];
//			byte[][] secondHalf = new byte[commands.length - (checkpointIndex + 1)][];
//			MessageContext[] secondHalfMsgCtx = new MessageContext[secondHalf.length];
//			System.arraycopy(commands, 0, firstHalf, 0, checkpointIndex + 1);
//			System.arraycopy(msgCtxs, 0, firstHalfMsgCtx, 0, checkpointIndex + 1);
//			if (secondHalf.length > 0) {
//				System.arraycopy(commands, checkpointIndex + 1, secondHalf, 0, commands.length - (checkpointIndex + 1));
//				System.arraycopy(msgCtxs, checkpointIndex + 1, secondHalfMsgCtx, 0,
//						commands.length - (checkpointIndex + 1));
//			} else {
//				firstHalfMsgCtx = msgCtxs;
//			}
//
////			byte[][] firstHalfReplies = new byte[firstHalf.length][];
////			byte[][] secondHalfReplies = new byte[secondHalf.length][];
//
//			// execute the first half
//			cid = msgCtxs[checkpointIndex].getConsensusId();
//
//			// add by zs
////			List<ReplyContextMessage> firstHalfReply = new ArrayList<>();
////			for (int i = 0, length = firstHalf.length; i < length; i++) {
////				firstHalfReply.add(replyContextMessages.get(i));
////			}
//
////			if (!noop) {
////				stateLock.lock();
////				if (firstHalfReply != null && !firstHalfReply.isEmpty()) {
////					firstHalfReplies = appExecuteBatch(firstHalf, firstHalfMsgCtx, true, firstHalfReply);
////				} else {
////					firstHalfReplies = appExecuteBatch(firstHalf, firstHalfMsgCtx, true);
////				}
////				stateLock.unlock();
////			}
//
//			saveCommands(commands, msgCtxs);
//			LOGGER.debug("(DefaultRecoverable.executeBatch) Performing checkpoint for consensus {}", cid);
//			stateLock.lock();
//			byte[] snapshot = getCheckPointSnapshot(cid);
//			stateLock.unlock();
//			saveState(snapshot, cid);
//
////            System.arraycopy(firstHalfReplies, 0, replies, 0, firstHalfReplies.length);
//
//			// execute the second half if it exists
//			if (secondHalf.length > 0) {
////	        	System.out.println("----THERE IS A SECOND HALF----");
//				cid = msgCtxs[msgCtxs.length - 1].getConsensusId();
//
//				// add by zs
////				List<ReplyContextMessage> secondHalfReply = new ArrayList<>();
////				for (int i = firstHalf.length; i < replyContextMessages.size(); i++) {
////					secondHalfReply.add(replyContextMessages.get(i));
////				}
//
////				if (!noop) {
////					stateLock.lock();
////					if (secondHalfReply != null && !secondHalfReply.isEmpty()) {
////						secondHalfReplies = appExecuteBatch(secondHalf, secondHalfMsgCtx, true, secondHalfReply);
////					} else {
////						secondHalfReplies = appExecuteBatch(secondHalf, secondHalfMsgCtx, true);
////					}
////					stateLock.unlock();
////				}
//
//				LOGGER.debug(
//						"(DefaultRecoverable.executeBatch) Storing message batch in the state log for consensus {}",
//						cid);
//				saveCommands(secondHalf, secondHalfMsgCtx);
//
////                System.arraycopy(secondHalfReplies, 0, replies, firstHalfReplies.length, secondHalfReplies.length);
//			}
//
//		}
//
//		if (cids != null && cids.length > 0) {
//			getStateManager().setLastCID(cids[cids.length - 1]);
//		}
//		return replies;
//	}


	// 目前的设计中commands只包含一轮共识批次的消息，故可以优化处理流程
	private byte[][] executeBatch(byte[][] commands, MessageContext[] msgCtxs, boolean noop) {

		if (commands.length > 0 && msgCtxs.length > 0 && commands.length == msgCtxs.length) {

			int cid = msgCtxs[0].getConsensusId();

			// cid 正好为检查点， checkpointPeriod,  2*checkpointPeriod, 3*checkpointPeriod,......
			if ((cid > 0) && (cid % checkpointPeriod == 0)) {
				stateLock.lock();
				byte[] snapshot = getCheckPointSnapshot(cid);
				stateLock.unlock();
				saveState(snapshot, cid);
			}

			saveCommands(commands, msgCtxs);

			getStateManager().setLastCID(cid);
		}

		return null;
	}

	public final byte[] computeHash(byte[] data) {
		byte[] ret = null;
		hashLock.lock();
		try {
			ret = md.hash(data);
		} finally {
			hashLock.unlock();
		}
		return ret;
	}

	private StateLog getLog() {
		initLog();
		return log;
	}

	private void saveState(byte[] snapshot, int lastCID) {
		try {

			StateLog thisLog = getLog();

			logLock.lock();

			LOGGER.debug("(TOMLayer.saveState) Saving state of CID {}", lastCID);

			thisLog.newCheckpoint(snapshot, computeHash(snapshot), lastCID);
			thisLog.setLastCID(lastCID);
			thisLog.setLastCheckpointCID(lastCID - 1);

			LOGGER.debug("(TOMLayer.saveState) Finished saving state of CID {}", lastCID);
		} finally {
			logLock.unlock();
		}
	}

	/**
	 * Write commands to log file
	 *
	 * @param commands array of commands. Each command is an array of bytes
	 * @param msgCtx
	 */
	private void saveCommands(byte[][] commands, MessageContext[] msgCtx) {
		// if(!config.isToLog())
		// return;
		if (commands.length != msgCtx.length) {
			LOGGER.error("----SIZE OF COMMANDS AND MESSAGE CONTEXTS IS DIFFERENT----");
			LOGGER.error("----COMMANDS: {}, CONTEXTS: {} ----", commands.length, msgCtx.length);
		}

		try {
			logLock.lock();

			int cid = msgCtx[0].getConsensusId();
			int batchStart = 0;
			for (int i = 0; i <= msgCtx.length; i++) {
				if (i == msgCtx.length) { // the batch command contains only one command or it is the last position of the
											// array
					byte[][] batch = Arrays.copyOfRange(commands, batchStart, i);
					MessageContext[] batchMsgCtx = Arrays.copyOfRange(msgCtx, batchStart, i);
					log.addMessageBatch(batch, batchMsgCtx, cid);
				} else {
					if (msgCtx[i].getConsensusId() > cid) { // saves commands when the cid changes or when it is the last
															// batch
						byte[][] batch = Arrays.copyOfRange(commands, batchStart, i);
						MessageContext[] batchMsgCtx = Arrays.copyOfRange(msgCtx, batchStart, i);
						log.addMessageBatch(batch, batchMsgCtx, cid);
						cid = msgCtx[i].getConsensusId();
						batchStart = i;
					}
				}
			}
		} finally {
			logLock.unlock();
		}
	}

	@Override
	public ApplicationState getState(int cid, boolean sendState) {

		try {

			logLock.lock();
			ApplicationState ret = (cid > -1 ? getLog().getApplicationState(cid, sendState)
					: new DefaultApplicationState());

			// Only will send a state if I have a proof for the last logged
			// decision/consensus
			// TODO: I should always make sure to have a log with proofs, since this is a
			// result
			// of not storing anything after a checkpoint and before logging more requests
//			if (ret == null || (config.isBFT() && ret.getCertifiedDecision(this.controller) == null)) {
			if (ret == null) { // 暂时没有办法考虑证据，由于证据没有上链，在节点重启时只加载了交易集，无法加载交易达成共识的证据
				ret = new DefaultApplicationState();
			}
			return ret;
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalStateException("[DefaultRecoverable] getState exception!");

		} finally {
			logLock.unlock();
		}
	}

	@Override
	public int setState(ApplicationState recvState) {
		int remoteLastCid = -1;
		DefaultApplicationState state = null;

		stateLock.lock();
		try {
			if (recvState instanceof DefaultApplicationState) {

				// 该状态是多数节点认可的
				state = (DefaultApplicationState) recvState;

				remoteLastCid = state.getLastCID();

				if (state.getSerializedState() != null) {
					initLog();
					LOGGER.info("The application state receive from remote is not null, update local app state!");
					log.update(state);
				}

				int localCid = ((StandardStateManager) this.getStateManager()).getTomLayer().getLastExec();

				LOGGER.info(
						"I am proc {}, my local cid = {}, remote checkpoint cid = {}, remote last cid = {}",
						controller.getStaticConf().getProcessId(), localCid, state.getLastCheckpointCID(), remoteLastCid);

				// 执行最新checkpoint的交易重放过程
				for (int cid = localCid + 1; cid <= remoteLastCid; cid++) {

					LOGGER.debug("[DefaultRecoverable.setState] interpreting and verifying batched requests for cid {}", cid);

					CommandsInfo cmdInfo = state.getMessageBatch(cid);

					if (cmdInfo != null) {

						LOGGER.info("I am proc {}, will do appExecuteBatch, cid = {}", controller.getStaticConf().getProcessId(), cid);

						appExecuteBatch(cmdInfo.commands, cmdInfo.msgCtx, false);

						((StandardStateManager) this.getStateManager()).setLastCID(cid);
						//更新上次执行的共识ID，同时把正在进行中的共识设置为-1
						((StandardStateManager) this.getStateManager()).getTomLayer().setLastExec(cid);
						((StandardStateManager) this.getStateManager()).getTomLayer().setInExec(-1);
					}
				}
			}
		} catch (Exception e) {
			LOGGER.error(
					"Error occurred while recovering the app state from remote! --" + e.getMessage(), e);
			if (e instanceof ArrayIndexOutOfBoundsException) {
				LOGGER.error(
						"CID do ultimo checkpoint: {}\r\n" + "CID do ultimo consenso: {}"
								+ "numero de mensagens supostamente no batch: {}\r\n"
								+ "numero de mensagens realmente no batch: {}",
						state.getLastCheckpointCID(), state.getLastCID(),
						(state.getLastCID() - state.getLastCheckpointCID() + 1),
						state.getMessageBatches().length);
			}
		} finally {
			stateLock.unlock();
		}

		return ((StandardStateManager) this.getStateManager()).getLastCID();
	}

	/**
	 * Iterates over the message context array and get the consensus id of each
	 * command being executed. As several times during the execution of commands and
	 * logging the only infomation important in MessageContext is the consensus id,
	 * it saves time to have it already in an array of ids
	 *
	 * @param ctxs the message context, one for each command to be executed
	 * @return the id of the consensus decision for each command
	 */
	private int[] consensusIds(MessageContext[] ctxs) {
		int[] cids = new int[ctxs.length];
		for (int i = 0; i < ctxs.length; i++) {
			cids[i] = ctxs[i].getConsensusId();
		}
		return cids;
	}

	/**
	 * Iterates over the commands to find if the replica took a checkpoint. This
	 * iteration over commands is needed due to the batch execution strategy
	 * introduced with the durable techniques to improve state management. As
	 * several consensus instances can be executed in the same batch of commands, it
	 * is necessary to identify if the batch contains checkpoint indexes.
	 *
	 * @param msgCtxs the contexts of the consensus where the messages where
	 *                executed. There is one msgCtx message for each command to be
	 *                executed
	 *
	 * @return the index in which a replica is supposed to take a checkpoint. If
	 *         there is no replica taking a checkpoint during the period comprised
	 *         by this command batch, it is returned -1
	 */
	private int findCheckpointPosition(int[] cids) {
		if (checkpointPeriod < 1) {
			return -1;
		}
		if (cids.length == 0) {
			throw new IllegalArgumentException();
		}
		int firstCID = cids[0];
		if ((firstCID + 1) % checkpointPeriod == 0) {
			return cidPosition(cids, firstCID);
		} else {
			int nextCkpIndex = (((firstCID / checkpointPeriod) + 1) * checkpointPeriod) - 1;
			if (nextCkpIndex <= cids[cids.length - 1]) {
				return cidPosition(cids, nextCkpIndex);
			}
		}
		return -1;
	}

	/**
	 * Iterates over the message contexts to retrieve the index of the last command
	 * executed prior to the checkpoint. That index is used by the state transfer
	 * protocol to find the position of the log commands in the log file.
	 *
	 * @param msgCtx the message context of the commands executed by the replica.
	 *               There is one message context for each command
	 * @param cid    the CID of the consensus where a replica took a checkpoint
	 * @return the higher position where the CID appears
	 */
	private int cidPosition(int[] cids, int cid) {
		int index = -1;
		if (cids[cids.length - 1] == cid) {
			return cids.length - 1;
		}
		for (int i = 0; i < cids.length; i++) {
			if (cids[i] > cid) {
				break;
			}
			index++;
		}
		LOGGER.debug("--- Checkpoint is in position {}", index);
		return index;
	}

	private void initLog() {
		if (log == null) {
			checkpointPeriod = config.getCheckpointPeriod();
			if (config.isToLog() && config.isLoggingToDisk()) {
		        int logLastConsensusId = -1;
				int replicaId = config.getProcessId();
				boolean isToLog = config.isToLog();
				boolean syncLog = config.isToWriteSyncLog();
				boolean syncCkp = config.isToWriteSyncCkp();
				log = new DiskStateLog(replicaId, null, null, isToLog, syncLog, syncCkp, this.realName, controller);

				logLastConsensusId = ((DiskStateLog) log).loadDurableState();

				byte[] state = getCheckPointSnapshot(log.getLastCheckpointCID());

				log.setState(state);

				log.setStateHash(computeHash(state));

				getStateManager().setLastCID(logLastConsensusId);

			} else {
				//Load latest checkpoint cycle txs into memory, to provide data sync for other backward nodes
				int lastCheckpointCid = -1;

				int lastCid = stateManager.getLastCID();

				if (lastCid < checkpointPeriod) {
					lastCheckpointCid = -1;
				} else {
					lastCheckpointCid = (lastCid / checkpointPeriod) * checkpointPeriod -1;
				}

				byte[] state = getCheckPointSnapshot(lastCheckpointCid);

				log = new StateLog(this.config.getProcessId(), checkpointPeriod, state, computeHash(state));
				log.setLastCheckpointCID(lastCheckpointCid);
				log.setLastCID(lastCid);

				for (int cid = lastCheckpointCid + 1; cid <= lastCid; cid++) {

					// 根据CID获取对应区块内的交易总数
					int currCidCommandsNum = getCommandsNumByCid(cid);

					byte[][] commands = new byte[currCidCommandsNum][];

					commands = getCommandsByCid(cid, currCidCommandsNum);

					MessageContext[] msgCtxs = new MessageContext[currCidCommandsNum];

					long blockTimeStamp = getTimestampByCid(cid);

					// 注意：MessageContext也需要持久化到账本，启动时从账本加载，否则缺失共识相关的proof!!!!!!!!!!!!!,暂不影响，待完善
					for (int i = 0; i < commands.length; i++) {
						msgCtxs[i] = new MessageContext(0, 0, null, 0, 0, 0, 0, null, blockTimeStamp, 0, 0, 0, 0, cid, null, null, false);
					}

					log.addMessageBatch(commands, msgCtxs, cid);
				}
			}
		}
	}

	@Override
	public void initContext(ReplicaContext replicaContext, long lastCid) {
		
		this.controller = replicaContext.getSVController();
		this.config = replicaContext.getStaticConfiguration();

		replicaContext.getTOMLayer().setLastExec((int) lastCid);

		getStateManager().setLastCID((int) lastCid);

		((StandardStateManager)getStateManager()).getTomLayer().setLastExec((int) lastCid);

		initLog();

		LOGGER.info("[DefaultRecoverable] initContext, procid = {}, lastCid = {}, lastCkpCid = {}", this.controller.getCurrentProcessId(), log.getLastCID(), log.getLastCheckpointCID());

		replicaContext.getTOMLayer().lastCidSetOk();
		
		getStateManager().askCurrentConsensusId();
	}

	@Override
	public StateManager getStateManager() {
		if (stateManager == null) {
			stateManager = new StandardStateManager();
		}
		return stateManager;
	}


	@Override
	public byte[] executeUnordered(byte[] command, MessageContext msgCtx) {
		return appExecuteUnordered(command, msgCtx);
	}

	@Override
	public void Op(int CID, byte[] requests, MessageContext msgCtx) {
		// Requests are logged within 'executeBatch(...)' instead of in this method.
	}

	@Override
	public void noOp(int CID, byte[][] operations, MessageContext[] msgCtxs) {

		executeBatch(operations, msgCtxs, true);

	}

	@Override
	public void setRealName(String realName) {
		this.realName = realName;
	}

	public abstract void installSnapshot(byte[] state);

	public abstract byte[] getCheckPointSnapshot(int cid);

	public abstract BatchAppResultImpl preComputeAppHash(int cid, byte[][] commands, long timestamp);

	public abstract List<byte[]> updateAppResponses(List<byte[]> asyncResponseLinkedList, byte[] commonHash,
			boolean isConsistent);

	public abstract void preComputeAppCommit(int cid, String batchId);

	public abstract void preComputeAppRollback(int cid, String batchId);

	public abstract byte[][] appExecuteBatch(byte[][] commands, MessageContext[] msgCtxs, boolean fromConsensus);

	public abstract byte[] appExecuteUnordered(byte[] command, MessageContext msgCtx);

	public abstract int getCommandsNumByCid(int cid);

	public abstract byte[][] getCommandsByCid(int cid, int currCidCommandsNum);

	public abstract long getTimestampByCid(int cid);

}
