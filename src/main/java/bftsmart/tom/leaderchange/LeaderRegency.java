package bftsmart.tom.leaderchange;

public class LeaderRegency {

	private int leader;

	private int regency;

	public LeaderRegency(int leader, int regency) {
		this.leader = leader;
		this.regency = regency;
	}

	public int getLeader() {
		return leader;
	}

	public int getRegency() {
		return regency;
	}
}