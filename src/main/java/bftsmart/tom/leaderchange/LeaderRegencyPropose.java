package bftsmart.tom.leaderchange;

import java.util.Arrays;

import bftsmart.reconfiguration.views.View;

/**
 * 描述全局所有节点的领导者执政信息；
 * 
 * @author huanghaiquan
 *
 */
public class LeaderRegencyPropose implements LeaderRegencyView{

	private LeaderRegency regency;

	private int viewId;

	private int[] viewProcessIds;

	private int sender;

	private int stateCode;

	/**
	 * 创建领导者选举提议；
	 * 
	 * @param regency        提议的下一个领导者
	 * @param viewId         视图Id；
	 * @param viewProcessIds 视图Id对应的进程列表；
	 * @param sender         提议的发起者；
	 */
	private LeaderRegencyPropose(LeaderRegency regency, int viewId, int[] viewProcessIds, int sender) {
		this.regency = regency;
		this.viewId = viewId;
		this.viewProcessIds = viewProcessIds;
		this.sender = sender;
	}

	/**
	 * 采取指定的 regencyId 在指定视图之上创建一个领导者执政期提议；
	 * <p>
	 * 
	 * 方法基于视图的进程 Id 列表的总数对 regencyId 求余，将结果作为下标从进程 Id 列表中取得新领导者节点的 Id；
	 * 
	 * @param regencyId 执政期 Id；
	 * @param view      视图；
	 * @param proposer  提议者 Id；
	 * @return
	 */
	public static LeaderRegencyPropose chooseFromView(int regencyId, View view, int proposer) {
		int[] viewProcessIds = view.getProcesses();
		Arrays.sort(viewProcessIds);
		int leaderId = chooseLeader(regencyId, viewProcessIds);
		return new LeaderRegencyPropose(new LeaderRegency(leaderId, regencyId), view.getId(), viewProcessIds, proposer);
	}

	public static LeaderRegencyPropose copy(int leaderId, int regencyId, int viewId, int[] viewProcessIds,
			int proposer) {
		viewProcessIds = viewProcessIds.clone();
		Arrays.sort(viewProcessIds);
		if (leaderId != chooseLeader(regencyId, viewProcessIds)) {
			throw new IllegalArgumentException(
					"The specified leader id do not match the choosing leader id from the specified view!");
		}
		return new LeaderRegencyPropose(new LeaderRegency(leaderId, regencyId), viewId, viewProcessIds, proposer);
	}

	/**
	 * 从指定的视图的节点 Id 列表中选择一个节点作为指定执政期的领导者；
	 * 
	 * @param regencyId
	 * @param viewProcessIds
	 * @return
	 */
	public static int chooseLeader(int regencyId, View view) {
		return chooseLeader(regencyId, view.getProcesses());
	}

	/**
	 * 从指定的视图的节点 Id 列表中选择一个节点作为指定执政期的领导者；
	 * 
	 * @param regencyId
	 * @param viewProcessIds
	 * @return
	 */
	private static int chooseLeader(int regencyId, int[] viewProcessIds) {
		int index = regencyId % viewProcessIds.length;
		int leaderId = viewProcessIds[index];
		return leaderId;
	}

	public LeaderRegency getRegency() {
		return regency;
	}
	
	@Override
	public int getLeaderId() {
		return regency.getLeaderId();
	}
	
	@Override
	public int getRegencyId() {
		return regency.getId();
	}
	
	@Override
	public int getNodeId() {
		return sender;
	}

	public int getSender() {
		return sender;
	}

	public int getViewId() {
		return viewId;
	}

	@Override
	public int getStateCode() {
		return stateCode;
	}

	public int[] getViewProcessIds() {
		return viewProcessIds.clone();
	}

	public boolean isViewEquals(View view) {
		return view.equals(viewId, this.viewProcessIds);
	}

	public boolean isViewEquals(LeaderRegencyPropose propose2) {
		return this.viewId == propose2.viewId && Arrays.equals(this.viewProcessIds, propose2.viewProcessIds);
	}
	
}