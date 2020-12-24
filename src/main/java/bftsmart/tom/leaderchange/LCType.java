package bftsmart.tom.leaderchange;

/**
 * 领导者切换的消息类型；
 * 
 * @author huanghaiquan
 *
 */
public enum LCType {

	/**
	 * 提议一轮新的选举；
	 */
	STOP(3, "STOP"),

	/**
	 * 附议一轮新的选举；
	 */
	STOP_APPEND(4, "STOP-APPEND"),

	/**
	 * 赞成一轮新的选举；
	 */
	STOP_DATA(5, "STOP-DATA"),

	/**
	 * 确认一轮新的选举；
	 */
	SYNC(6, "SYNC");

	public final int CODE;
	
	public final String NAME;

	private LCType(int code, String name) {
		this.CODE = code;
		this.NAME = name;
	}

	public static LCType valueOf(int code) {
		for (LCType type : values()) {
			if (type.CODE == code) {
				return type;
			}
		}
		throw new IllegalArgumentException("No LCType enum with code[" + code + "]!");
	}

}
