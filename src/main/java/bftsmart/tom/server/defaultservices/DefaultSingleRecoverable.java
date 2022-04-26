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

import bftsmart.consensus.app.SHA256Utils;
import bftsmart.reconfiguration.ViewTopology;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.StateManager;
import bftsmart.statemanagement.strategy.StandardStateManager;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ReplicaConfiguration;
import bftsmart.tom.ReplicaContext;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.server.SingleExecutable;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class provides a basic state transfer protocol using the interface
 * 'SingleExecutable'.
 * @author Marcel Santos
 */
public abstract class DefaultSingleRecoverable implements Recoverable, SingleExecutable {
    
    protected ReplicaContext replicaContext;
    private ReplicaConfiguration config;
    private ViewTopology controller;
    private int checkpointPeriod;

    private ReentrantLock logLock = new ReentrantLock();
    private ReentrantLock hashLock = new ReentrantLock();
    private ReentrantLock stateLock = new ReentrantLock();
    
    private SHA256Utils md = new SHA256Utils();
        
    private StateLog log;
    private List<byte[]> commands = new ArrayList<>();
    private List<MessageContext> msgContexts = new ArrayList<>();
    private String realName;
    private StateManager stateManager;

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(DefaultSingleRecoverable.class);
    
    public DefaultSingleRecoverable() {

//        try {
//            md = MessageDigest.getInstance("MD5"); // TODO: shouldn't it be SHA?
//        } catch (NoSuchAlgorithmException ex) {
//            java.util.logging.Logger.getLogger(DefaultSingleRecoverable.class.getName()).log(Level.SEVERE, null, ex);
//        }
    }
    
    @Override
    public byte[] executeOrdered(byte[] command, MessageContext msgCtx) {
        
        return executeOrdered(command, msgCtx, false);
        
    }
    
    private byte[] executeOrdered(byte[] command, MessageContext msgCtx, boolean noop) {
        
        int cid = msgCtx.getConsensusId();
        
        byte[] reply = null;
            
        if (!noop) {
            stateLock.lock();
            reply = appExecuteOrdered(command, msgCtx);
            stateLock.unlock();
        }
        
        commands.add(command);
        msgContexts.add(msgCtx);
        
        if(msgCtx.isLastInBatch()) {
	        if ((cid > 0) && ((cid % checkpointPeriod) == 0)) {
	           LOGGER.debug("(DefaultSingleRecoverable.executeOrdered) Performing checkpoint for consensus {}", cid);
	            stateLock.lock();
	            byte[] snapshot = getBlockHashByCid(cid);
	            stateLock.unlock();
	            saveState(snapshot, cid);
	        } else {
	            saveCommands(commands.toArray(new byte[0][]), msgContexts.toArray(new MessageContext[0]));
	        }
			getStateManager().setLastCID(cid);
	        commands = new ArrayList<>();
                msgContexts = new ArrayList<>();
        }
        return reply;
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
        StateLog thisLog = getLog();

        logLock.lock();

       LOGGER.debug("(TOMLayer.saveState) Saving state of CID {}", lastCID);

        thisLog.newCheckpoint(snapshot, computeHash(snapshot), lastCID);
        thisLog.setLastCID(-1);
        thisLog.setLastCheckpointCID(lastCID);

        logLock.unlock();
        /*System.out.println("fiz checkpoint");
        System.out.println("tamanho do snapshot: " + snapshot.length);
        System.out.println("tamanho do log: " + thisLog.getMessageBatches().length);*/
       LOGGER.debug("(TOMLayer.saveState) Finished saving state of CID {}", lastCID);
    }

    private void saveCommands(byte[][] commands, MessageContext[] msgCtx) {
        
        if (commands.length != msgCtx.length) {
            LOGGER.error("----SIZE OF COMMANDS AND MESSAGE CONTEXTS IS DIFFERENT----");
            LOGGER.error("----COMMANDS: {}, CONTEXTS: {} ----", commands.length, msgCtx.length);
        }
        logLock.lock();

        int cid = msgCtx[0].getConsensusId();
        int batchStart = 0;
        for (int i = 0; i <= msgCtx.length; i++) {
            if (i == msgCtx.length) { // the batch command contains only one command or it is the last position of the array
                byte[][] batch = Arrays.copyOfRange(commands, batchStart, i);
                MessageContext[] batchMsgCtx = Arrays.copyOfRange(msgCtx, batchStart, i);
                log.addMessageBatch(batch, batchMsgCtx, cid);
            } else {
                if (msgCtx[i].getConsensusId() > cid) { // saves commands when the CID changes or when it is the last batch
                    byte[][] batch = Arrays.copyOfRange(commands, batchStart, i);
                    MessageContext[] batchMsgCtx = Arrays.copyOfRange(msgCtx, batchStart, i);
                    log.addMessageBatch(batch, batchMsgCtx, cid);
                    cid = msgCtx[i].getConsensusId();
                    batchStart = i;
                }
            }
        }
        
        logLock.unlock();
    }

    @Override
    public ApplicationState getState(int cid, boolean sendState) {

        try {
            logLock.lock();
            ApplicationState ret = (cid > -1 ? getLog().getApplicationState(cid, sendState) : new DefaultApplicationState());

            // Only will send a state if I have a proof for the last logged decision/consensus
            //TODO: I should always make sure to have a log with proofs, since this is a result
            // of not storing anything after a checkpoint and before logging more requests
            if (ret == null || (config.isBFT() && ret.getCertifiedDecision(this.controller) == null)) ret = new DefaultApplicationState();

            LOGGER.debug("Getting log until CID {}, null? {} ", cid, (ret == null));
            return ret;
        } catch (Exception e) {
            LOGGER.error("[DefaultSingleRecoverable] getState exception, {}", e.getMessage());
            throw new IllegalStateException("[DefaultSingleRecoverable] getState exception!");
        } finally {
            logLock.unlock();
        }
    }
    
    @Override
    public int setState(ApplicationState recvState) {
        int lastCID = -1;
        if (recvState instanceof DefaultApplicationState) {
            
            DefaultApplicationState state = (DefaultApplicationState) recvState;
            
            LOGGER.debug("(DefaultSingleRecoverable.setState) last CID in state: {}", state.getLastCID());
            
            logLock.lock();
            initLog();
            log.update(state);
            logLock.unlock();
            
            int lastCheckpointCID = state.getLastCheckpointCID();
            
            lastCID = state.getLastCID();

            LOGGER.debug("(DefaultSingleRecoverable.setState) I'm going to update myself from CID {} to CID"
                    , lastCheckpointCID, lastCID);

            stateLock.lock();
            installSnapshot(state.getState());

            for (int cid = lastCheckpointCID + 1; cid <= lastCID; cid++) {
                try {
                    LOGGER.debug("(DurabilityCoordinator.setState) interpreting and verifying batched requests for CID {}", cid);

                    CommandsInfo cmdInfo = state.getMessageBatch(cid);
                    byte[][] cmds = cmdInfo.commands; // take a batch
                    MessageContext[] msgCtxs = cmdInfo.msgCtx;
                    
                    if (cmds == null || msgCtxs == null || msgCtxs[0].isNoOp()) {
                        continue;
                    }
                    
                    for(int i = 0; i < cmds.length; i++) {
                    	appExecuteOrdered(cmds[i], msgCtxs[i]);
                    }
                } catch (Exception e) {
                    e.printStackTrace(System.err);
                    if (e instanceof ArrayIndexOutOfBoundsException) {
                        LOGGER.error("CID do ultimo checkpoint: {}", state.getLastCheckpointCID());
                        LOGGER.error("CID do ultimo consenso: {}", state.getLastCID());
                        LOGGER.error("numero de mensagens supostamente no batch: {}", (state.getLastCID() - state.getLastCheckpointCID() + 1));
                        LOGGER.error("numero de mensagens realmente no batch: {}", state.getMessageBatches().length);
                    }
                }
            }
            stateLock.unlock();

        }

        return lastCID;
    }

    @Override
    public void initContext(ReplicaContext replicaContext, long lastCid) {
        this.replicaContext = replicaContext;
        this.config = replicaContext.getStaticConfiguration();
        this.controller = replicaContext.getSVController();

        if (log == null) {
            checkpointPeriod = config.getCheckpointPeriod();
            if (config.isToLog() && config.isLoggingToDisk()) {
                int replicaId = config.getProcessId();
                boolean isToLog = config.isToLog();
                boolean syncLog = config.isToWriteSyncLog();
                boolean syncCkp = config.isToWriteSyncCkp();
                log = new DiskStateLog(replicaId, null,null, isToLog, syncLog, syncCkp, this.realName, controller);
                int logLastConsensusId = ((DiskStateLog) log).loadDurableState();

                byte[] state = getBlockHashByCid(log.getLastCheckpointCID());

                if (logLastConsensusId > 0) {
//                    setState(storedState);
                    getStateManager().setLastCID(logLastConsensusId);
                }
            } else {
                log = new StateLog(this.config.getProcessId(), checkpointPeriod, null, null);
            }
        }
        getStateManager().askCurrentConsensusId();
    }
    /*@Override
    public void setReplicaContext(ReplicaContext replicaCtx) {
            this.replicaContext = replicaCtx;
    this.config = replicaCtx.getStaticConfiguration();
    }*/

    @Override
    public StateManager getStateManager() {
    	if(stateManager == null)
    		stateManager = new StandardStateManager();
    	return stateManager;
    }
	
    private void initLog() {
    	if(log == null) {
    		checkpointPeriod = config.getCheckpointPeriod();
            if(config.isToLog() && config.isLoggingToDisk()) {
            	int replicaId = config.getProcessId();
            	boolean isToLog = config.isToLog();
            	boolean syncLog = config.isToWriteSyncLog();
            	boolean syncCkp = config.isToWriteSyncCkp();
            	log = new DiskStateLog(replicaId, null, null, isToLog, syncLog, syncCkp, this.realName, controller);
            	((DiskStateLog) log).loadDurableState();
                byte[] state = getBlockHashByCid(log.getLastCheckpointCID());
                log.setState(state);
                log.setStateHash(computeHash(state));
            } else
            	log = new StateLog(controller.getStaticConf().getProcessId(), checkpointPeriod, null, null);
    	}
    }
    
        
        
    @Override
    public byte[] executeUnordered(byte[] command, MessageContext msgCtx) {
        return appExecuteUnordered(command, msgCtx);
    }
    
    @Override
    public void Op(int CID, byte[] requests, MessageContext msgCtx) {
        //Requests are logged within 'executeOrdered(...)' instead of in this method.
    }

    @Override
    public void noOp(int CID, byte[][] operations, MessageContext[] msgCtx) {
         
        for (int i = 0; i < msgCtx.length; i++) {
            executeOrdered(operations[i], msgCtx[i], true);
        }
    }

    @Override
    public void setRealName(String realName) {
        this.realName = realName;
    }

    public abstract void installSnapshot(byte[] state);
    
    public abstract byte[] getBlockHashByCid(int cid);
    
    public abstract byte[] appExecuteOrdered(byte[] command, MessageContext msgCtx);
    
    public abstract byte[] appExecuteUnordered(byte[] command, MessageContext msgCtx);
}
