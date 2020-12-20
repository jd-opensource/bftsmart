package bftsmart.tom.leaderchange;

public enum LeaderStatus {

	OK(0),

	TIMEOUT(1);

	public final int CODE;

	private LeaderStatus(int code) {
		this.CODE = code;
	}

	public static LeaderStatus valueOf(int code) {
		for (LeaderStatus status : LeaderStatus.values()) {
			if (status.CODE == code) {
				return status;
			}
		}
		throw new IllegalArgumentException("Unsupported code[" + code + "]!");
	}

}
