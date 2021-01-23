package bftsmart.util;

public final class ConsensusUtils {

	
	/**
	 * 计算在指定节点数量下的拜占庭容错数；
	 * 
	 * @param n
	 * @return
	 */
	public static int computeBFT_F(int n) {
		if (n < 1) {
			throw new IllegalArgumentException("Node number is less than 1!");
		}
		int f = 0;
		while (true) {
			if (n >= (3 * f + 1) && n < (3 * (f + 1) + 1)) {
				break;
			}
			f++;
		}
		return f;
	}

	public static int computeCFT_F(int n) {
		if (n < 1) {
			throw new IllegalArgumentException("Node number is less than 1!");
		}
		int f = 0;
		while (true) {
			if (n >= (2 * f + 1) && n < (2 * (f + 1) + 1)) {
				break;
			}
			f++;
		}
		return f;
	}

}
