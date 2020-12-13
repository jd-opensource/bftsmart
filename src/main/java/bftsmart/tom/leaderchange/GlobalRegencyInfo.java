package bftsmart.tom.leaderchange;

/**
 * 描述全局所有节点的领导者执政信息；
 * 
 * @author huanghaiquan
 *
 */
public class GlobalRegencyInfo {

	private int minLeaderId;

	private int maxRegency;

	private boolean consistant;

//		private int[] processIds;
//		
//		private int[] leaderIds;
//		
//		private int[] regencies;

	public GlobalRegencyInfo(int minLeaderId, int maxRegency, boolean consistant) {
		this.minLeaderId = minLeaderId;
		this.maxRegency = maxRegency;
		this.consistant = consistant;
	}

	public int getMinLeaderId() {
		return minLeaderId;
	}

	public int getMaxRegency() {
		return maxRegency;
	}

	public boolean isConsistant() {
		return consistant;
	}

}