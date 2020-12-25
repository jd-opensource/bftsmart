package bftsmart.tom.leaderchange;

import bftsmart.communication.SystemMessage;
import bftsmart.reconfiguration.views.View;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * Message used during leader change and synchronization
 * 
 * @author shaozhuguang
 */
public class LeaderResponseMessage extends SystemMessage implements LeaderRegencyView {

	private long sequence;

	private int leader;

	private int lastRegency;

	private int viewId;

	private int[] viewProcessIds;

	public LeaderResponseMessage() {
	}

	/**
	 * Constructor
	 * 
	 * @param from   replica that creates this message
	 * @param leader type of the message (STOP, SYNC, CATCH-UP)
	 */
	public LeaderResponseMessage(int from, LeaderRegency regency, View view, long sequence) {
		super(from);
		this.lastRegency = regency.getId();
		this.leader = regency.getLeaderId();
		this.viewId = view.getId();
		this.viewProcessIds = view.getProcesses();
		this.sequence = sequence;
	}

	public int getLeaderId() {
		return leader;
	}

	public void setLeaderId(int leader) {
		this.leader = leader;
	}

	public long getSequence() {
		return sequence;
	}

	public void setSequence(long sequence) {
		this.sequence = sequence;
	}

	public int getLastRegency() {
		return lastRegency;
	}

	public void setLastRegency(int lastRegency) {
		this.lastRegency = lastRegency;
	}

	public int getViewId() {
		return viewId;
	}

	public int[] getViewProcessIds() {
		return viewProcessIds;
	}

	@Override
	public int getRegencyId() {
		return lastRegency;
	}

	@Override
	public int getNodeId() {
		return sender;
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);

		out.writeInt(leader);
		out.writeInt(lastRegency);
		out.writeInt(viewId);
		out.writeObject(viewProcessIds);
		out.writeLong(sequence);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);

		leader = in.readInt();
		lastRegency = in.readInt();
		viewId = in.readInt();
		viewProcessIds = (int[]) in.readObject();
		Arrays.sort(viewProcessIds);
		sequence = in.readLong();

	}
}
