package bftsmart.tom.leaderchange;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 选举结果；
 * 
 * @author huanghaiquan
 *
 */
public class ElectionResult {

	private LeaderRegency regency;

	private LeaderRegencyPropose[] validProposes;

	private LeaderRegencyPropose[] totalProposes;

	private ElectionResult(LeaderRegency regency, LeaderRegencyPropose[] validProposes,
			LeaderRegencyPropose[] totalProposes) {
		this.regency = regency;
		this.validProposes = validProposes;
		this.totalProposes = totalProposes;
		if (validProposes == null || validProposes.length == 0) {
			throw new IllegalArgumentException("No valid propose!");
		}
	}

	/**
	 * 有效的提议数；
	 * <p>
	 * 
	 * @return
	 */
	public int getValidCount() {
		return validProposes.length;
	}

	/**
	 * 选举结果所基于的视图 Id；
	 * 
	 * @return
	 */
	public int getViewId() {
		// 从一致的提议清单中取 view id；
		return validProposes[0].getViewId();
	}

	public int[] getViewProcessIds() {
		// 从一致的提议清单中取 view ；
		return validProposes[0].getViewProcessIds();
	}

	public LeaderRegency getRegency() {
		return regency;
	}

	public LeaderRegencyPropose[] getValidProposes() {
		return validProposes;
	}

	public LeaderRegencyPropose[] getTotalProposes() {
		return totalProposes;
	}

	/**
	 * 根据指定的提议生成选举结果；
	 * <p>
	 * 
	 * 方法将检查所有选举提议的一致性：<br>
	 * 1. 是否在一致的视图下，提出了一致的领导者执政期提议；<br>
	 * 2. 满足一致性的提议数量是否满足法定数量的要求；<br>
	 * 
	 * <p>
	 * 选举结果中有效的提议是在最高的视图上形成的一致的提议；
	 * <p>
	 * 
	 * 因此，返回的选举结果中至少会有 1 项有效的提议；
	 * 
	 * @param proposes
	 * @return
	 */
	public static ElectionResult generateResult(int regency, LeaderRegencyPropose[] proposes) {
		assert proposes != null && proposes.length > 0;

		List<LeaderRegencyPropose> validProposeList = new ArrayList<LeaderRegencyPropose>();

		LeaderRegencyPropose standard = proposes[0];
		if (regency != standard.getRegency().getId()) {
			throw new IllegalStateException("The regency of propose from node[" + standard.getSender()
					+ "] is not the expected regency[" + regency + "]!");
		}
		validProposeList.add(standard);

		for (int i = 1; i < proposes.length; i++) {
			if (regency != proposes[i].getRegency().getId()) {
				throw new IllegalStateException("The regency of propose from node[" + proposes[i].getSender()
						+ "] is not the expected regency[" + regency + "]!");
			}

			if (proposes[i].getViewId() < standard.getViewId()) {
				// 忽略视图小的提议；
				continue;
			}

			// 统一对齐到最大的视图，统计在最大视图上的投票数；
			if (proposes[i].getViewId() > standard.getViewId()) {
				// 发现更大的视图，重置对齐标注和计数器；
				standard = proposes[i];
				validProposeList.clear();
				validProposeList.add(standard);
				continue;
			}

			// 与标准提议具有相同的视图 Id；
			// 验证视图 Id 列表是否一致；
			if (!standard.isViewEquals(proposes[i])) {
				// 如果来自不同节点的提议具有相同的视图 Id，却有不同的 process id 列表，则抛出异常；
				String errorMessage = String.format("The leader regency proposes from node[%s] and node[%s] "
						+ "have the same view id[%s] but diferent process id list! \r\n--View processes of node[%s]=%s\r\nView processes of node[%s]=%s",
						standard.getSender(), proposes[i].getSender(), standard.getViewId(), //
						standard.getSender(), Arrays.toString(standard.getViewProcessIds()), //
						proposes[i].getSender(), Arrays.toString(proposes[i].getViewProcessIds()));
				throw new IllegalStateException(errorMessage);
			}

			// 由于 LeaderRegencyPropose 的实现保证了在相同的 view process id 列表上，相同的 regency
			// 总是能够选举出相同leader;
			// 所以此处不必重复验证 leader 的一致性；
			validProposeList.add(proposes[i]);
		}

		LeaderRegencyPropose[] validProposes = validProposeList
				.toArray(new LeaderRegencyPropose[validProposeList.size()]);
		return new ElectionResult(standard.getRegency(), validProposes, proposes);
	}

}
