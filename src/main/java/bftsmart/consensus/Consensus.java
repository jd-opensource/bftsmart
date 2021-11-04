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
package bftsmart.consensus;

import bftsmart.reconfiguration.ViewTopology;
import bftsmart.tom.core.ExecutionManager;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;


/**
 * This class stands for a consensus instance that implements the algorithm
 * for the Byzantine fault model described in Cachin's 'Yet Another Visit to Paxos' (April 2011)
 */
public class Consensus {

    private ExecutionManager manager; // Execution manager for this replica's consensus instances

    private Decision decision; // Decision instance to which this consensus works for
    private HashMap<Integer,Epoch> epochs = new HashMap<Integer,Epoch>(2);
    private ReentrantLock epochsLock = new ReentrantLock(); // Lock for concurrency control
    private ReentrantLock writeSetLock = new ReentrantLock(); //lock for this consensus write set

    private boolean decided; // Is this consensus decided?
    private int decisionEpoch = -1; // epoch at which a decision was made

    //NEW ATTRIBUTES FOR THE LEADER CHANGE
    private int ets = 0;
    private TimestampValuePair quorumWrites = null;
    private HashSet<TimestampValuePair> writeSet = new HashSet<TimestampValuePair>();

    private boolean precomputeRolled = false;

    private boolean precomputed = false;

    private boolean precomputeCommited = false;

    private boolean secondTimeout = false;

    public ReentrantLock lock = new ReentrantLock(); //this consensus lock (called by other classes)

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Consensus.class);
    
    /**
     * Creates a new instance of Consensus
     * 
     * Important: At this point, the 'decision' parameter is only a placeholder
     * for the future decision, and should not be delivered to the delivery thread.
     * 
     * Use 'isDecided()' to check if the consensus already decided a value.
     * 
     * @param manager Execution manager for this replica's consensus instances
     * @param decision Decision instance to which this consensus works for.
     */
    public Consensus(ExecutionManager manager, Decision decision) {
        this.manager = manager;
        this.decision = decision;
    }

    /**
     * This is the consensus ID
     * @return Consensus ID
     */
    public int getId() {
        return decision.getConsensusId();
    }

    /**
     * Has there been a precompute rollback happen?
     * @return false/true
     */
    public boolean getPrecomputeRolled() {
        return precomputeRolled;
    }

    /**
     * Has there been a precompute happen?
     * @return false/true
     */
    public boolean getPrecomputed() {
        return precomputed;
    }

    /**
     * @return consensus's all epochs
     */
    public HashMap<Integer,Epoch> getEpochs() {
        return epochs;
    }

    /**
     * Has there been a precompute commit happen?
     * @return false/true
     */
    public boolean getPrecomputeCommited() {
        return precomputeCommited;
    }

    /**
     * Has there been the second timeout happen?
     * @return false/true
     */
//    public boolean getSecondTimeout() {
//        return secondTimeout;
//    }

    /**
     * set precompute roll happened
     * @return void
     */
    public void setPrecomputeRolled() {
        precomputeRolled = true;
    }

    /**
     * set precompute happened
     * @return void
     */
    public void setPrecomputed(boolean precomputed) {
        this.precomputed = precomputed;
    }

    /**
     * set precompute commit happened
     * @return void
     */
    public void setPrecomputeCommited(boolean precomputeCommited) {
        this.precomputeCommited = precomputeCommited;
    }

    /**
     * The second timeout happened
     * @return void
     */
//    public void setSecondTimeout(boolean secondTimeout) {
//        this.secondTimeout = secondTimeout;
//    }

    /**
     * This is the execution manager for this replica's consensus instances
     * @return Execution manager for this replica
     */
    public ExecutionManager getManager() {
        return manager;
    }

    /**
     * This is the decision instance to which this consensus works for
     * 
     * Important: The returned object should only be sent to the delivery thread
     * after this consensus instance decides a value. Use 'isDecided()' to check if
     * the consensus already decided a value.
     * 
     * @return Decision instance to which this consensus works for
     */
    public Decision getDecision() {
        return decision;
    }

    /**
     * Gets a epoch associated with this consensus
     * @param timestamp The timestamp of the epoch
     * @param controller The view controller for the replicas
     * @return The epoch
     */
    public Epoch getEpoch(int timestamp, ViewTopology controller) {
        return getEpoch(timestamp,true, controller);
    }

    /**
     * Gets a epoch associated with this consensus
     * @param timestamp The number of the epoch
     * @param create if the epoch is to be created if not existent
     * @param controller The view controller for the replicas
     * @return The epoch
     */
    public Epoch getEpoch(int timestamp, boolean create, ViewTopology controller) {
        epochsLock.lock();

        Epoch epoch = epochs.get(timestamp);
        if(epoch == null && create){
            epoch = new Epoch(controller, this, timestamp);
            epochs.put(timestamp, epoch);
//            System.out.printf("getEpoch i am %s, time = %s \r\n",
//                    controller.getStaticConf().getProcessId(), timestamp);
        }

        epochsLock.unlock();

        return epoch;
    }
    
    /**
     * Increments the ETS of this consensus, thus advancing 
     * to the next epoch
     */
    public void incEts() {
        ets++;
    }
    
    /**
     * Increments the ETS of this consensus, thus advancing 
     * to the next epoch
     * 
     * @param ets New ETS for this consensus, to advance
     * to the next epoch. It must be greater than the current ETS
     */
    public void setETS(int ets) {
        
        if (ets > this.ets) this.ets = ets;
    }
    
    /**
     * Returns the timestamp for the current epoch
     * @return the timestamp for the current epoch
     */
    public int getEts() {
        return ets;
    }
    
    /**
     * Store the value read from a Byzantine quorum of WRITES
     * @param value
     */
    public void setQuorumWrites(byte[] value) {

        quorumWrites = new TimestampValuePair(ets, value);
    }

    /**
     * Return the value read from a Byzantine quorum of WRITES that has
     * previously been stored
     * @return the value read from a Byzantine quorum of WRITES, if such
     * value has been obtained already
     */
    public TimestampValuePair getQuorumWrites() {
        return quorumWrites;
    }

    /**
     * Add a value that shall be written to the writeSet
     * @param value Value to write to the writeSet
     */
    public void addWritten(byte[] value) {

        writeSetLock.lock();
        writeSet.add(new TimestampValuePair(ets, value));
        writeSetLock.unlock();
    }

    /**
     * Remove an already writte value from  writeSet
     * @param value valor a remover do writeSet
     */
    public void removeWritten(byte[] value) {

        writeSetLock.lock();
        
        Set<TimestampValuePair> temp = (HashSet<TimestampValuePair>) writeSet.clone();
        
        for (TimestampValuePair rv : temp) {

            if (Arrays.equals(rv.getValue(), value)) writeSet.remove(rv);
        }
        writeSetLock.unlock();

    }
    public HashSet<TimestampValuePair> getWriteSet() {
        return (HashSet<TimestampValuePair>) writeSet.clone(); 
    }
    /**
     * Creates an epoch associated with this consensus, with the specified timestamp
     * @param timestamp The timestamp to associated to this epoch
     * @param recManager The replica's ServerViewController
     * @return The epoch
     */
    public Epoch createEpoch(int timestamp, ViewTopology recManager) {
        epochsLock.lock();

        Epoch epoch = new Epoch(recManager, this, timestamp);
        epochs.put(timestamp, epoch);

        LOGGER.debug("createEpoch i am {}, time = {}",
                manager.getTOMLayer().controller.getStaticConf().getProcessId(), timestamp);

        epochsLock.unlock();

        return epoch;
    }
    /**
     * Creates a epoch associated with this consensus, supposedly the next
     * @param recManager The replica's ServerViewController
     * @return The epoch
     */
    public Epoch createEpoch(ViewTopology recManager) {
        epochsLock.lock();

        Set<Integer> keys = epochs.keySet();

        int max = -1;
        for (int k : keys) {
            if (k > max) max = k;
        }

        max++;
        Epoch epoch = new Epoch(recManager, this, max);
        epochs.put(max, epoch);

        epochsLock.unlock();

        return epoch;
    }

    /**
     * Removes epochs greater than 'limit' from this consensus instance
     *
     * @param limit Epochs that should be kept (from 0 to 'limit')
     */
    public void removeEpochs(int limit) {
        epochsLock.lock();

        for(Integer key : (Integer[])epochs.keySet().toArray(new Integer[0])) {
            if(key > limit) {
                Epoch epoch = epochs.remove(key);
                epoch.setRemoved();
                //epoch.getTimeoutTask().cancel();
            }
        }

        epochsLock.unlock();
    }

    /**
     * The epoch at which a decision was possible to make
     * @return Epoch at which a decision was possible to make
     */
    public Epoch getDecisionEpoch() {
        epochsLock.lock();
        Epoch e = epochs.get(decisionEpoch);
        epochsLock.unlock();
        return e;
    }

    /**
     * The last epoch of this consensus instance
     *
     * @return Last epoch of this consensus instance
     */
    public Epoch getLastEpoch() {
        epochsLock.lock();
        if (epochs.isEmpty()) {
            epochsLock.unlock();
            return null;
        }
        //Epoch epoch = epochs.get(epochs.size() - 1);
        Epoch epoch = epochs.get(ets); // the last epoch corresponds to the current ETS
        epochsLock.unlock();
        return epoch;
    }

    /**
     * Informs whether or not the consensus instance has decided a value
     *
     * @return True if it is decided, false otherwise
     */
    public boolean isDecided() {
        return decided;
    }

    /**
     * Called by the Acceptor object, to set the decided value
     *
     * @param epoch The epoch at which a decision was made
     * @param deliver Set to true to deliver decision to TOMLayer/DeliveryThread
     */
    public void decided(Epoch epoch, boolean deliver) {
        if (!decided) {
            decided = true;
            decisionEpoch = epoch.getTimestamp();
            decision.setDecisionEpoch(epoch);
            decision.setRollback(epoch.getRollback());
            if (deliver) {
                LOGGER.debug("(Consensus.decided) Delivering decision from consensus {} to the TOMLayer/DeliveryThread", getId());
//                System.out.println("(Consensus.decided) Delivering decision from consensus " + getId() + " to the TOMLayer/DeliveryThread, cid = "
//                        + epoch.getConsensus().getId() + ", i am " + manager.getTOMLayer().controller.getStaticConf().getProcessId());
                manager.getTOMLayer().decided(decision);
            }
        }
    }
}
