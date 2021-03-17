package bftsmart.tom.leaderchange;

/**
 * @Author: zhangshuang
 * @Date: 2021/3/15 4:20 PM
 * Version 1.0
 * LC切换过程中节点的时间戳状态对
 *
 */
public class LCTimestampStatePair {


    private long ts;

    private LCState lcState;

    public LCTimestampStatePair(long ts, LCState lcState) {
       this.ts = ts;
       this.lcState = lcState;
    }

    /**
     * 设置状态时的时间戳
     *
     * @return
     */
    public long getTs() {
        return ts;
    }

    /**
     * LC状态
     *
     * @return
     */
    public LCState getLcState() {
        return lcState;
    }

    public void setLcState(LCState lcState) {
        this.lcState = lcState;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }
}
