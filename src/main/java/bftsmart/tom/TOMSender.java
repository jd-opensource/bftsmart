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
package bftsmart.tom;

import bftsmart.communication.client.CommunicationSystemClientSide;
import bftsmart.communication.client.CommunicationSystemClientSideFactory;
import bftsmart.communication.client.ReplyReceiver;
import bftsmart.reconfiguration.ClientViewController;
import bftsmart.reconfiguration.util.TOMConfiguration;
import bftsmart.reconfiguration.views.ViewStorage;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import utils.net.SSLSecurity;

import java.io.Closeable;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is used to multicast messages to replicas and receive replies.
 */
public abstract class TOMSender implements ReplyReceiver, Closeable, AutoCloseable {

	private int me; // process id

	private ClientViewController viewController;

	private int session = 0; // session id
	private AtomicInteger sequence = new AtomicInteger(0); // sequence number
	private AtomicInteger unorderedMessageSequence = new AtomicInteger(0); // sequence number for readonly messages
	private CommunicationSystemClientSide cs; // Client side comunication system
	private Lock lock = new ReentrantLock(); // lock to manage concurrent access to this object by other threads
	private boolean useSignatures = false;
	private AtomicInteger opCounter = new AtomicInteger(0);

	/**
	 * Creates a new instance of TOMulticastSender
	 *
	 * TODO: This may really be empty?
	 */
	public TOMSender() {
	}

	public void close(){
		cs.close();
	}

	public CommunicationSystemClientSide getCommunicationSystem() {
		return this.cs;
	}


	//******* EDUARDO BEGIN **************//
	public ClientViewController getViewManager(){
		return this.viewController;
	}

	public void init(TOMConfiguration config, ViewStorage viewStorage, SSLSecurity sslSecurity) {
		this.viewController = new ClientViewController(config, viewStorage);
		startsCS(viewController.getStaticConf().getProcessId(), sslSecurity);
	}

	private void startsCS(int clientId, SSLSecurity sslSecurity) {
		this.cs = CommunicationSystemClientSideFactory.getCommunicationSystemClientSide(clientId, this.viewController, sslSecurity);
		this.cs.setReplyReceiver(this); // This object itself shall be a reply receiver
		this.me = this.viewController.getStaticConf().getProcessId();
		this.useSignatures = this.viewController.getStaticConf().isUseSignatures();
		this.session = new Random().nextInt();
	}
	//******* EDUARDO END **************//


	public int getProcessId() {
		return me;
	}

	protected int generateRequestId(TOMMessageType type) {
//		lock.lock();
		int id;
		if(type == TOMMessageType.ORDERED_REQUEST || type == TOMMessageType.RECONFIG)
			id = sequence.getAndIncrement();
		else
			id = unorderedMessageSequence.getAndIncrement(); 
//		lock.unlock();

		return id;
	}

	public int generateOperationId() {
		return opCounter.getAndIncrement();
	}

	public void TOMulticast(TOMMessage sm) {
		cs.send(useSignatures, this.viewController.getCurrentViewProcesses(), sm);
	}


	public void TOMulticast(byte[] m, int reqId, int operationId, TOMMessageType reqType) {
		cs.send(useSignatures, viewController.getCurrentViewProcesses(),
				new TOMMessage(me, session, reqId, operationId, m, null, viewController.getCurrentViewId(),
						reqType));
	}


	public void sendMessageToTargets(byte[] m, int reqId, int operationId, int[] targets, TOMMessageType type) {
		// remove TTP;
//		if(this.getViewManager().getStaticConf().isTheTTP()) {
//			type = TOMMessageType.ASK_STATUS;
//		}
		cs.send(useSignatures, targets,
				new TOMMessage(me, session, reqId, operationId, m, null, viewController.getCurrentViewId(), type));
	}

	public int getSession(){
		return session;
	}
}
