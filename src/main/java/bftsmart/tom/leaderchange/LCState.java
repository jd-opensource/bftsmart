package bftsmart.tom.leaderchange;

/**
 * 领导者切换过程中的状态类型；
 *
 *
 */
public enum LCState {

	/**
	 * 节点尚未触发选举流程；
	 */
	NORMAL(1, "NORMAL"),

	/**
	 * 节点处于开始选举且未同步的状态
	 */
	LC_SELECTING(2, "LC-SELECTING"),

	/**
	 * 节点处于选举同步，但未成功结束的状态
	 */
	LC_SYNC(3, "LC-SYNC");

	public final int CODE;

	public final String NAME;

	private LCState(int code, String name) {
		this.CODE = code;
		this.NAME = name;
	}

	public static LCState valueOf(int code) {
		for (LCState type : values()) {
			if (type.CODE == code) {
				return type;
			}
		}
		throw new IllegalArgumentException("No LCState enum with code[" + code + "]!");
	}

}
