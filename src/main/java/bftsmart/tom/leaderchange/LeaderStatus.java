package bftsmart.tom.leaderchange;

public class LeaderStatus extends LeaderRegency{

	private int status;


	public LeaderStatus(int status, int leader, int regency) {
		super(leader, regency);
		this.status = status;
	}

	public int getStatus() {
		return status;
	}
}