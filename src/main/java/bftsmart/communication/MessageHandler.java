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
package bftsmart.communication;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;

import bftsmart.statemanagement.strategy.StandardTRMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.consensus.messages.MessageFactory;
import bftsmart.consensus.roles.Acceptor;
import bftsmart.reconfiguration.views.View;
import bftsmart.statemanagement.SMMessage;
import bftsmart.tom.core.TOMLayer;
import bftsmart.tom.core.messages.ForwardedMessage;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.ViewMessage;
import bftsmart.tom.leaderchange.HeartBeatMessage;
import bftsmart.tom.leaderchange.LCMessage;
import bftsmart.tom.leaderchange.LeaderRequestMessage;
import bftsmart.tom.leaderchange.LeaderResponseMessage;
import bftsmart.tom.leaderchange.LeaderStatusRequestMessage;
import bftsmart.tom.leaderchange.LeaderStatusResponseMessage;
import bftsmart.tom.util.TOMUtil;

/**
 *
 * @author edualchieri
 */
public class MessageHandler {
	private static final Logger LOGGER = LoggerFactory.getLogger(MessageHandler.class);

	private Acceptor acceptor;
	private TOMLayer tomLayer;

	public MessageHandler() {
	}

	public void setAcceptor(Acceptor acceptor) {
		this.acceptor = acceptor;
	}

	public void setTOMLayer(TOMLayer tomLayer) {
		this.tomLayer = tomLayer;
	}

	@SuppressWarnings("unchecked")
	public void processData(SystemMessage sm) {
		if (sm instanceof ConsensusMessage) {

			int myId = tomLayer.controller.getStaticConf().getProcessId();

			ConsensusMessage consMsg = (ConsensusMessage) sm;

			if (consMsg.getSender() == myId //
					|| (!tomLayer.controller.getStaticConf().isUseMACs()) //
					|| consMsg.authenticated) {

				acceptor.deliver(consMsg);

			} else if (consMsg.getType() == MessageFactory.ACCEPT && consMsg.getProof() != null) {

				// We are going to verify the MAC vector at the algorithm level
				HashMap<Integer, byte[]> macVector = (HashMap<Integer, byte[]>) consMsg.getProof();

				byte[] recvMAC = macVector.get(myId);

				ConsensusMessage cm = new ConsensusMessage(MessageFactory.ACCEPT, consMsg.getNumber(),
						consMsg.getEpoch(), consMsg.getSender(), consMsg.getValue());

				ByteArrayOutputStream bOut = new ByteArrayOutputStream(248);
				try {
					new ObjectOutputStream(bOut).writeObject(cm);
				} catch (IOException ex) {
					throw new IllegalStateException("Error occurred while serializing ConsensusMessage[" + cm.toString()
							+ "]! --" + ex.getMessage(), ex);
				}

				byte[] data = bOut.toByteArray();

				// byte[] hash = tomLayer.computeHash(data);

				byte[] myMAC = null;
				MacMessageCodec<SystemMessage> msgCodec = tomLayer.getCommunication().getServersCommunication().getMessageCodec(consMsg.getSender());
				MacKey macKey = msgCodec.getMacKey();
				if (macKey != null) {
					myMAC = macKey.generateMac(data);
				}

				if (recvMAC != null && myMAC != null && Arrays.equals(recvMAC, myMAC))
					acceptor.deliver(consMsg);
				else {
					LOGGER.error("(MessageHandler.processData) WARNING: invalid MAC from {}", sm.getSender());
				}
			} else {
				LOGGER.error("(MessageHandler.processData) Discarding unauthenticated message from {}", sm.getSender());
			}

		} else if (sm instanceof HeartBeatMessage) {
			// 心跳消息
			tomLayer.heartBeatTimer.receiveHeartBeatMessage((HeartBeatMessage) sm);
		} else if (sm instanceof ViewMessage) {
			// 视图消息
			// 通过该消息可更新本地视图
			ViewMessage viewMessage = (ViewMessage) sm;
			int remoteId = viewMessage.getSender();
			View view = viewMessage.getView();
			if (view != null) {
				tomLayer.viewSyncTimer.updateView(remoteId, view);
			}
		} else if (sm instanceof LeaderRequestMessage) {
			// 获取Leader节点请求的消息
			tomLayer.heartBeatTimer.receiveLeaderRequestMessage((LeaderRequestMessage) sm);
		} else if (sm instanceof LeaderResponseMessage) {
			// 获取Leader节点请求的消息
			tomLayer.heartBeatTimer.receiveLeaderResponseMessage((LeaderResponseMessage) sm);
		} else if (sm instanceof LeaderStatusResponseMessage) {
			// 该处理顺序必须在前面，因为LeaderStatusResponseMessage继承自LeaderStatusRequestMessage
			tomLayer.heartBeatTimer.receiveLeaderStatusResponseMessage((LeaderStatusResponseMessage) sm);
		} else if (sm instanceof LeaderStatusRequestMessage) {
			tomLayer.heartBeatTimer.receiveLeaderStatusRequestMessage((LeaderStatusRequestMessage) sm);
		} else {
			if ((!tomLayer.controller.getStaticConf().isUseMACs()) || sm.authenticated) {
				/*** This is Joao's code, related to leader change */
				if (sm instanceof LCMessage) {
					LCMessage lcMsg = (LCMessage) sm;
					tomLayer.getSynchronizer().deliverTimeoutRequest(lcMsg);
					/**************************************************************/

				} else if (sm instanceof ForwardedMessage) {
					TOMMessage request = ((ForwardedMessage) sm).getRequest();
					tomLayer.requestReceived(request);

					/** This is Joao's code, to handle state transfer */
				} else if (sm instanceof SMMessage) {
					SMMessage smsg = (SMMessage) sm;
					LOGGER.debug("I am {}, receive SMMessage[{}], type = {} !",
							tomLayer.controller.getStaticConf().getProcessId(), smsg.getSender(), smsg.getType());
					switch (smsg.getType()) {
					case TOMUtil.SM_REQUEST:
						tomLayer.getStateManager().SMRequestDeliver(smsg, tomLayer.controller.getStaticConf().isBFT());
						break;
					case TOMUtil.SM_REPLY:
						tomLayer.getStateManager().SMReplyDeliver(smsg, tomLayer.controller.getStaticConf().isBFT());
						break;
					case TOMUtil.SM_ASK_INITIAL:
						tomLayer.getStateManager().currentConsensusIdAsked(smsg.getSender(), smsg.getView().getId());
						break;
					case TOMUtil.SM_REPLY_INITIAL:
						tomLayer.getStateManager().currentConsensusIdReceived(smsg);
						break;
					default:
						tomLayer.getStateManager().stateTimeout();
						break;
					}
					/******************************************************************/
				} else if (sm instanceof StandardTRMessage) {

					StandardTRMessage trMessage = (StandardTRMessage)sm;

					LOGGER.info("I am {}, receive StandardTRMessage[{}], type = {} !",
							tomLayer.controller.getStaticConf().getProcessId(), trMessage.getSender(), trMessage.getType());

					switch (trMessage.getType()) {
						case TOMUtil.SM_TRANSACTION_REPLAY_REQUEST_INFO:
							tomLayer.getStateManager().transactionReplayAsked(trMessage.sender, trMessage.getTarget(), trMessage.getStartCid(), trMessage.getEndCid());
							break;
						case TOMUtil.SM_TRANSACTION_REPLAY_REPLY_INFO:
							tomLayer.getStateManager().transactionReplayReplyDeliver(trMessage);
							break;
						default:
							break;

					}
				} else {
					LOGGER.error("UNKNOWN MESSAGE TYPE: {}", sm);
				}
			} else {
				LOGGER.error("(MessageHandler.processData) Discarding unauthenticated message from {}", sm.getSender());
			}
		}
	}

	protected void verifyPending() {
		tomLayer.processOutOfContext();
		tomLayer.processOutOfContextWriteAndAccept();
	}

	public Acceptor getAcceptor() {
		return acceptor;
	}
}
