package bftsmart.tom.leaderchange;

import java.util.Arrays;

/**
 * 描述全局所有节点的领导者执政信息；
 * 
 * @author huanghaiquan
 *
 */
public class LeaderRegencyPropose {

	private LeaderRegency regency;

	private int viewId;

	private int sender;

	private int[] viewProcessIds;

	/**
	 * 创建领导者选举提议；
	 * 
	 * @param regency        提议的下一个领导者
	 * @param viewId         视图Id；
	 * @param viewProcessIds 视图Id对应的进程列表；
	 * @param sender
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
	 * @param regencyId
	 * @param viewId
	 * @param viewProcessIds
	 * @param sender
	 * @return
	 */
	public static LeaderRegencyPropose proposeNewRegency(int regencyId, int viewId, int[] viewProcessIds, int sender) {
		viewProcessIds = viewProcessIds.clone();
		Arrays.sort(viewProcessIds);
		int leaderIndex = regencyId % viewProcessIds.length;
		int leaderId = viewProcessIds[leaderIndex];

		return new LeaderRegencyPropose(new LeaderRegency(leaderId, regencyId), viewId, viewProcessIds, sender);
	}

	public static LeaderRegencyPropose create(int leaderId, int regencyId, int viewId, int[] viewProcessIds,
			int sender) {
		viewProcessIds = viewProcessIds.clone();
		Arrays.sort(viewProcessIds);
		return new LeaderRegencyPropose(new LeaderRegency(leaderId, regencyId), viewId, viewProcessIds, sender);

	}

	public LeaderRegency getRegency() {
		return regency;
	}

	public int getSender() {
		return sender;
	}

	public int getViewId() {
		return viewId;
	}

	public int[] getViewProcessIds() {
		return viewProcessIds.clone();
	}
}