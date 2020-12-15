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

import bftsmart.communication.server.ServerConnection;
import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.consensus.messages.MessageFactory;
import bftsmart.consensus.roles.Acceptor;
import bftsmart.reconfiguration.views.View;
import bftsmart.statemanagement.SMMessage;
import bftsmart.tom.core.TOMLayer;
import bftsmart.tom.core.messages.ForwardedMessage;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.ViewMessage;
import bftsmart.tom.leaderchange.*;
import bftsmart.tom.util.TOMUtil;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;

/**
 *
 * @author edualchieri
 */
public class MessageHandler {

	private Acceptor acceptor;
	private TOMLayer tomLayer;
	// private Cipher cipher;
	private Mac mac;
	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(MessageHandler.class);

	public MessageHandler() {
		try {
			// this.cipher = Cipher.getInstance("DES/ECB/PKCS5Padding");
			this.mac = Mac.getInstance(ServerConnection.MAC_ALGORITHM);
		} catch (NoSuchAlgorithmException /* | NoSuchPaddingException */ ex) {
			ex.printStackTrace();
		}
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

//            System.out.printf("(MessageHandler) node %s receive consensus msg from %s , type is = %s, time = %s \r\n",
//                    tomLayer.controller.getStaticConf().getProcessId(), sm.getSender(), consMsg.getType(), System.currentTimeMillis());

			if (tomLayer.controller.getStaticConf().getUseMACs() == 0 || consMsg.authenticated
					|| consMsg.getSender() == myId)
				acceptor.deliver(consMsg);
			else if (consMsg.getType() == MessageFactory.ACCEPT && consMsg.getProof() != null) {

				// We are going to verify the MAC vector at the algorithm level
				HashMap<Integer, byte[]> macVector = (HashMap<Integer, byte[]>) consMsg.getProof();

				byte[] recvMAC = macVector.get(myId);

				ConsensusMessage cm = new ConsensusMessage(MessageFactory.ACCEPT, consMsg.getNumber(),
						consMsg.getEpoch(), consMsg.getSender(), consMsg.getValue());

				ByteArrayOutputStream bOut = new ByteArrayOutputStream(248);
				try {
					new ObjectOutputStream(bOut).writeObject(cm);
				} catch (IOException ex) {
					ex.printStackTrace();
				}

				byte[] data = bOut.toByteArray();

				// byte[] hash = tomLayer.computeHash(data);

				byte[] myMAC = null;

				/*
				 * byte[] k =
				 * tomLayer.getCommunication().getServersConn().getSecretKey(paxosMsg.getSender(
				 * )).getEncoded(); SecretKeySpec key = new SecretKeySpec(new
				 * String(k).substring(0, 8).getBytes(), "DES");
				 */

				SecretKey key = tomLayer.getCommunication().getServersConn().getSecretKey(consMsg.getSender());
				try {
					this.mac.init(key);
					myMAC = this.mac.doFinal(data);
				} catch (/* IllegalBlockSizeException | BadPaddingException | */ InvalidKeyException ex) {
					ex.printStackTrace();
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
			if (tomLayer.controller.getStaticConf().getUseMACs() == 0 || sm.authenticated) {
				/*** This is Joao's code, related to leader change */
				if (sm instanceof LCMessage) {
					LCMessage lcMsg = (LCMessage) sm;
//	                LOGGER.debug("(MessageHandler.processData) LC_MSG received: type " + type + ", regency " + lcMsg.getReg() + ", (replica " + lcMsg.getSender() + ")");
					if (lcMsg.TRIGGER_LC_LOCALLY) {
						tomLayer.requestsTimer.run_lc_protocol(null);
					} else {
						tomLayer.getSynchronizer().deliverTimeoutRequest(lcMsg);
					}
					/**************************************************************/

				} else if (sm instanceof ForwardedMessage) {
					TOMMessage request = ((ForwardedMessage) sm).getRequest();
					tomLayer.requestReceived(request);

					/** This is Joao's code, to handle state transfer */
				} else if (sm instanceof SMMessage) {
					SMMessage smsg = (SMMessage) sm;
					LOGGER.info("I am {}, receive SMMessage[{}], type = {} !",
							tomLayer.controller.getStaticConf().getProcessId(), smsg.getSender(), smsg.getType());
					// LOGGER.debug("(MessageHandler.processData) SM_MSG received: type " +
					// smsg.getType() + ", regency " + smsg.getRegency() + ", (replica " +
					// smsg.getSender() + ")");
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
				} else {
					LOGGER.error("UNKNOWN MESSAGE TYPE: {}", sm);
				}
			} else {
				LOGGER.error("(MessageHandler.processData) Discarding unauthenticated message from {}", sm.getSender());
			}
		}
	}

	protected void verifyPending() {
		if (!tomLayer.getStateManager().isRetrievingState()) {
			tomLayer.processOutOfContext();
			tomLayer.processOutOfContextWriteAndAccept();
		}
	}
}
