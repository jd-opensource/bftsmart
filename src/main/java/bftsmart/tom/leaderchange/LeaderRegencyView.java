package bftsmart.tom.leaderchange;

import java.util.Arrays;

/**
 * 领导者执政期视图；
 * <p>
 * 
 * 描述某一个特定节点上的执政期信息；
 * 
 * @author huanghaiquan
 *
 */
public interface LeaderRegencyView {

	/**
	 * 领导者 Id；
	 * 
	 * @return
	 */
	int getLeaderId();

	/**
	 * 执政期 Id；
	 * 
	 * @return
	 */
	int getRegencyId();

	/**
	 * 领导者执政期；
	 * 
	 * @return
	 */
	default LeaderRegency getRegency() {
		return new LeaderRegency(getLeaderId(), getRegencyId());
	}

	/**
	 * 视图 Id；
	 * 
	 * @return
	 */
	int getViewId();

	/**
	 * 视图的进程 Id 列表；
	 * 
	 * @return
	 */
	int[] getViewProcessIds();

	/**
	 * LC 状态码信息；
	 *
	 * @return
	 */
	int getStateCode();

	/**
	 * 当前实例表示的领导者执政期视图所在的节点；
	 * 
	 * @return
	 */
	int getNodeId();

	default boolean isViewEquals(LeaderRegencyView regencyView) {
		return this.getViewId() == regencyView.getViewId()
				&& Arrays.equals(this.getViewProcessIds(), regencyView.getViewProcessIds());
	}
}
