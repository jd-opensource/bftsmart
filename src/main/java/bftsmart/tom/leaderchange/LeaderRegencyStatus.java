package bftsmart.tom.leaderchange;

public class LeaderRegencyStatus extends LeaderRegency{

	private LeaderStatus status;


	public LeaderRegencyStatus(LeaderStatus status, int leader, int regency) {
		super(leader, regency);
		this.status = status;
	}

	public LeaderStatus getStatus() {
		return status;
	}
}