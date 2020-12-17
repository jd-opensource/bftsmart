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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import bftsmart.communication.SystemMessage;
import bftsmart.reconfiguration.views.View;

/**
 * Message used during leader change and synchronization
 * 
 * @author Joao Sousa
 */
public class LCMessage extends SystemMessage {

	private LCType type;
	private int leader;
	private int regency;
	private int viewId;
	private int[] viewProcessIds;
	private byte[] payload;
	public final boolean TRIGGER_LC_LOCALLY; // indicates that the replica should
												// initiate the LC protocol locally

	/**
	 * Empty constructor
	 */
	public LCMessage() {
		this.TRIGGER_LC_LOCALLY = false;
	}

	/**
	 * Constructor
	 * 
	 * @param from    replica that creates this message
	 * @param type    type of the message (STOP, SYNC, CATCH-UP)
	 * @param leader  the leader id of the current regency in the replica created
	 *                this message;
	 * @param regency timestamp of leader change and synchronization; besides, it is
	 *                the new regency will be elected;
	 * @param payload dada that comes with the message
	 */
	private LCMessage(int from, LCType type, int leader, int regency, int viewId, int[] viewProcessIds,
			byte[] payload) {
		super(from);
		this.type = type;
		this.leader = leader;
		this.regency = regency;
		this.viewId = viewId;
		this.viewProcessIds = viewProcessIds;// 通过静态的工厂方法传入的对象已经是克隆和排序后的结果；

		this.payload = payload == null ? new byte[0] : payload;
//		if (type == TOMUtil.TRIGGER_LC_LOCALLY && from == -1)
//			this.TRIGGER_LC_LOCALLY = true;
//		else
		this.TRIGGER_LC_LOCALLY = false;
	}

	public static LCMessage createSTOP(int from, LeaderRegency regency, View view, byte[] payload) {
		return new LCMessage(from, LCType.STOP, regency.getLeaderId(), regency.getId(), view.getId(),
				view.getProcesses(), payload);
	}

	public static LCMessage createSTOP_APPEND(int from, LeaderRegency regency, View view, byte[] payload) {
		return new LCMessage(from, LCType.STOP_APPEND, regency.getLeaderId(), regency.getId(), view.getId(),
				view.getProcesses(), payload);
	}

	public static LCMessage createSTOP_DATA(int from, ElectionResult electionResult, byte[] payload) {
		return new LCMessage(from, LCType.STOP_DATA, electionResult.getRegency().getLeaderId(),
				electionResult.getRegency().getId(), electionResult.getViewId(), electionResult.getViewProcessIds(),
				payload);
	}

	public static LCMessage createSYNC(int from, LeaderRegency regency, View view, byte[] payload) {
		return new LCMessage(from, LCType.SYNC, regency.getLeaderId(), regency.getId(), view.getId(),
				view.getProcesses(), payload);
	}

	/**
	 * Get type of message
	 * 
	 * @return type of message
	 */
	public LCType getType() {
		return type;
	}

	/**
	 * Get timestamp of leader change and synchronization
	 * 
	 * @return timestamp of leader change and synchronization
	 */
	public int getReg() {
		return regency;
	}

	/**
	 * the leader id of the current regency in the replica created this message;
	 * 
	 * @return
	 */
	public int getLeader() {
		return leader;
	}

	public int getViewId() {
		return viewId;
	}

	public int[] getViewProcessIds() {
		return viewProcessIds;
	}

	/**
	 * Obter data of the message
	 * 
	 * @return data of the message
	 */
	public byte[] getPayload() {
		return payload;
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);

		out.writeInt(type.CODE);
		out.writeInt(leader);
		out.writeInt(regency);
		out.writeInt(viewId);
		out.writeObject(viewProcessIds);
		out.writeObject(payload);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);

		type = LCType.valueOf(in.readInt());
		leader = in.readInt();
		regency = in.readInt();
		viewId = in.readInt();
		viewProcessIds = (int[]) in.readObject();
		payload = (byte[]) in.readObject();
	}
}
