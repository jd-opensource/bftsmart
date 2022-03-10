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
package bftsmart.tom.server.defaultservices;

import bftsmart.reconfiguration.ViewTopology;
import bftsmart.tom.MessageContext;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class DiskStateLog extends StateLog {

	private int id;
	public  String DEFAULT_DIR = "";

	private static final int INT_BYTE_SIZE = 4;
	private static final int EOF = 0;

	private RandomAccessFile log;
	private RandomAccessFile ckp;

	private boolean syncLog;
	private String logPath;
	private String lastCkpPath;
	private boolean syncCkp;
	private boolean isToLog;
	private String realName;
	private ViewTopology controller;
	private String logDefaultFile;
	private String ckpDefaultFile;
	private ReentrantLock checkpointLock = new ReentrantLock();
	private Map<Integer, Long> logPointers;

	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(DiskStateLog.class);
	
	public DiskStateLog(int id, byte[] initialState, byte[] initialHash,
						boolean isToLog, boolean syncLog, boolean syncCkp, String realName, ViewTopology controller) {
		super(id, initialState, initialHash);
		this.id = id;
		this.isToLog = isToLog;
		this.syncLog = syncLog;
		this.syncCkp = syncCkp;
		this.realName = realName;
		this.controller = controller;
		this.logPointers = new HashMap<>();

		if (DEFAULT_DIR.length() == 0) {
			try {
				File libDir = new File(DiskStateLog.class.getProtectionDomain().getCodeSource().getLocation().getPath());
				LOGGER.info("DiskStateLog's lib path = {} !", libDir.getAbsolutePath());
				DEFAULT_DIR = libDir.getParentFile().getParentFile().getPath();
				this.logDefaultFile = File.separator + "runtime" + File.separator + this.realName + "." + id + ".txs" + ".log";
				this.ckpDefaultFile = File.separator + "runtime" + File.separator + this.realName + "." + id + ".txs" + ".ckp";
			} catch (Exception e) {
				LOGGER.error("load runtime path error !", e);
			}
		}
		LOGGER.info("Default dir = {}", DEFAULT_DIR);

	}

	private void createLogFile() {
		try {
			logPath = DEFAULT_DIR + logDefaultFile;
			log = new RandomAccessFile(logPath, (syncLog ? "rwd" : "rw"));
			// PreAllocation
			/*
			 * log.setLength(TEN_MB); log.seek(0);
			 */
		} catch (Exception e) {
			LOGGER.error("create log file error !", e);
		}
	}

	/**
	 * Adds a message batch to the log. This batches should be added to the log
	 * in the same order in which they are delivered to the application. Only
	 * the 'k' batches received after the last checkpoint are supposed to be
	 * kept
	 * 
	 * @param commands The batch of messages to be kept.
         * @param consensusId
	 */
        @Override
	public void addMessageBatch(byte[][] commands, MessageContext[] msgCtx, int consensusId) {
		CommandsInfo command = new CommandsInfo(commands, msgCtx);
		if (isToLog) {
			if (log == null || logPath == null)
				createLogFile();
			writeCommandToDisk(command, consensusId);
		}
		setLastCID(consensusId);
	}

	private void writeCommandToDisk(CommandsInfo commandsInfo, int consensusId) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try {
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(commandsInfo);
			oos.flush();

			byte[] batchBytes = bos.toByteArray();

			ByteBuffer bf = ByteBuffer.allocate(3 * INT_BYTE_SIZE
					+ batchBytes.length);
			bf.putInt(batchBytes.length);
			bf.put(batchBytes);
			bf.putInt(EOF);
			bf.putInt(consensusId);

			LOGGER.info("I am proc {}, Write command to disk, cid = {}", id, consensusId);

			// avoid node restart, disk file will be overwrite
			if (log.length() > 2 * INT_BYTE_SIZE) {
				log.seek(log.length() - 2 * INT_BYTE_SIZE);// Next write will overwrite
			}
			log.write(bf.array());
													// the EOF mark
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOGGER.error("write command to disk error !", e);
	    }
	}

        @Override
	public void newCheckpoint(byte[] state, byte[] stateHash, int consensusId) {
		String ckpPath = DEFAULT_DIR + File.separator + "runtime" + File.separator + this.realName + "." + String.valueOf(id) + ".txs" + ".tmp";
		try {
			checkpointLock.lock();
			RandomAccessFile ckp = new RandomAccessFile(ckpPath,
					(syncCkp ? "rwd" : "rw"));

			ByteBuffer bf = ByteBuffer.allocate(state.length + stateHash.length
					+ 4 * INT_BYTE_SIZE);
			bf.putInt(state.length);
			bf.put(state);
			bf.putInt(stateHash.length);
			bf.put(stateHash);
			bf.putInt(EOF);
			bf.putInt(consensusId);

			byte[] ckpState = bf.array();
			
			ckp.write(ckpState);
			ckp.close();

			if (isToLog)
				deleteLogFile();
			deleteLastCkp();
			renameCkp(ckpPath);
			if (isToLog)
				createLogFile();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			LOGGER.error("new checkpoint error !", e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOGGER.error("new checkpoint error !", e);
		} finally {
			checkpointLock.unlock();
		}
	}

	private void renameCkp(String ckpPath) {
		String finalCkpPath = ckpPath.replace(".tmp", ".ckp");
		new File(ckpPath).renameTo(new File(finalCkpPath));
		lastCkpPath = finalCkpPath;
	}

	private void deleteLastCkp() {
		try {
			if (ckp != null)
				ckp.close();
			if (lastCkpPath != null) {
				new File(lastCkpPath).delete();
			}
		} catch (IOException e) {
			LOGGER.error("delete last ckp error !", e);
		}
	}

	private void deleteLogFile() {
		try {
			if(log != null)
				log.close();
			if (logPath != null) {
				new File(logPath).delete();
			}
		} catch (IOException e) {
			LOGGER.error("delete log file error !", e);
		}
	}

	/**
	 * Prepare lastCheckpointCID by load runtime/tx.ckp file or according to lastCid to compute
	 *
	 * @param int lastCid
	 * @return lastCheckpointCID
	 */
	private int prepareLastCheckpointCID(int lastCid) {

		int lastCheckpointCID = getLastCheckpointCID();

		int computeCheckPointCID = this.controller.getStaticConf().getCheckpointPeriod() * (lastCid / this.controller.getStaticConf().getCheckpointPeriod()) - 1;

		if (lastCheckpointCID != computeCheckPointCID) {
			lastCheckpointCID = computeCheckPointCID;
			setLastCheckpointCID(lastCheckpointCID);
		};
		return lastCheckpointCID;
	}

	/**
	 * Constructs a TransferableState using this log information
	 * 
	 * @param cid Consensus ID correspondent to desired state
         * @param sendState
	 * @return TransferableState Object containing this log information
	 */
        @Override
	public DefaultApplicationState getApplicationState(int cid, boolean sendState) {
//		readingState = true;
		CommandsInfo[] batches = null;

		int lastCID = getLastCID();

		int lastCheckpointCID = prepareLastCheckpointCID(lastCID);

		LOGGER.info("I AM PROC {}, LAST CKP CID = {}", id, lastCheckpointCID);
		LOGGER.info("I AM PROC {}, CID = {}", id, cid);
		LOGGER.info("I AM PROC {}, LAST CID = {}", id, lastCID);
		if (cid >= lastCheckpointCID && cid <= lastCID) {

			int size = cid - lastCheckpointCID;

			FileRecoverer fr = new FileRecoverer(id, DEFAULT_DIR);

//			if (size > 0 && sendState) {
			if (size > 0) {
				CommandsInfo[] recoveredBatches = fr.getLogState(size, logPath);

				if (recoveredBatches == null || recoveredBatches.length == 0) {
					LOGGER.info("recoveredBatches == null || recoveredBatches.length == 0");
					return null;
				}
				batches = new CommandsInfo[recoveredBatches.length];


				for (int i = 0; i < recoveredBatches.length; i++)
					batches[i] = recoveredBatches[i];
			}
			
			checkpointLock.lock();
			byte[] ckpState = null, ckpStateHash = null;
			if (lastCkpPath != null) {
				ckpState = fr.getCkpState(lastCkpPath);
				ckpStateHash = fr.getCkpStateHash();
			} else {
				ckpState = super.getState();
				ckpStateHash = super.getStateHash();
			}
			checkpointLock.unlock();

			LOGGER.debug("--- FINISHED READING STATE");
//			readingState = false;

//			return new DefaultApplicationState((sendState ? batches : null), lastCheckpointCID,
			return new DefaultApplicationState(batches, lastCheckpointCID,
					cid, (sendState ? ckpState : null), ckpStateHash, this.id);

		}
		return null;
	}
	
	public void transferApplicationState(SocketChannel sChannel, int cid) {
		FileRecoverer fr = new FileRecoverer(id, DEFAULT_DIR);
		fr.transferCkpState(sChannel, lastCkpPath);
//		int lastCheckpointCID = getLastCheckpointCID();
//		int lastCID = getLastCID();
//		if (cid >= lastCheckpointCID && cid <= lastCID) {
//			int size = cid - lastCheckpointCID;
//			fr.transferLog(sChannel, size);
//		}
	}

	public void setLastCID(int cid, int checkpointPeriod, int checkpointPortion) {
		super.setLastCID(cid);
		// save the file pointer to retrieve log information later
		if((cid % checkpointPeriod) % checkpointPortion == checkpointPortion -1) {
			int ckpReplicaIndex = (((cid % checkpointPeriod) + 1) / checkpointPortion) -1;
			try {
				LOGGER.debug(" --- Replica {} took checkpoint. My current log pointer is {}", ckpReplicaIndex, log.getFilePointer());
				logPointers.put(ckpReplicaIndex, log.getFilePointer());
			} catch (IOException e) {
				LOGGER.error("set last CID error !", e);
			}
		}
	}

	/**
	 * Updates this log, according to the information contained in the
	 * TransferableState object
	 * 
	 * @param transState
	 *            TransferableState object containing the information which is
	 *            used to updated this log
	 */
        @Override
	public void update(DefaultApplicationState transState) {
//		newCheckpoint(transState.getState(), transState.getStateHash(), transState.getLastCheckpointCID());
		setLastCheckpointCID(transState.getLastCheckpointCID());
	}
	
	protected int loadDurableState() {
		int ckpLastConsensusId = -1;
		int logLastConsensusId = -1;

		File ckpFile = new File(DEFAULT_DIR + ckpDefaultFile);
		if (ckpFile.exists()) {
			lastCkpPath = DEFAULT_DIR + ckpDefaultFile;
		}

        File logFile = new File(DEFAULT_DIR + logDefaultFile);
        if (logFile.exists()) {
            logPath = DEFAULT_DIR + logDefaultFile;
        }

		if(lastCkpPath != null) {
			try {
				ckp = new RandomAccessFile(lastCkpPath, (syncCkp ? "rwd" : "rw"));
				ckp.seek(ckp.length() - INT_BYTE_SIZE);
				ckpLastConsensusId = ckp.readInt();
			} catch (Exception e) {
				LOGGER.error("load durable state error !", e);
			}
		}

		if(logPath !=null) {
        	try {
                log = new RandomAccessFile(logPath, (syncLog ? "rwd" : "rw"));
                log.seek(log.length() - INT_BYTE_SIZE);
                logLastConsensusId = log.readInt();
			} catch (Exception e) {
				LOGGER.error("load durable state error !", e);
			}
		}

		if(logLastConsensusId > ckpLastConsensusId) {
			super.setLastCID(logLastConsensusId);
		} else
			super.setLastCID(ckpLastConsensusId);

		prepareLastCheckpointCID(super.getLastCID());

		LOGGER.info("[DiskStateLog] loadDurableState, procid = {}, logLastConsensusId = {}, ckpLastConsensusId = {}", id, logLastConsensusId, ckpLastConsensusId);

		return logLastConsensusId;
	}
}
