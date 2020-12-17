package bftsmart.tom.leaderchange;

/**
 * 领导者执政期；
 * 
 * @author huanghaiquan
 *
 */
public class LeaderRegency {

	private int id;

	private int leaderId;

	public LeaderRegency(int leaderId, int regencyId) {
		this.leaderId = leaderId;
		this.id = regencyId;
	}

	/**
	 * 执政期 Id；
	 * 
	 * @return
	 */
	public int getId() {
		return id;
	}

	/**
	 * 领导者节点的 Id；
	 * 
	 * @return
	 */
	public int getLeaderId() {
		return leaderId;
	}
}