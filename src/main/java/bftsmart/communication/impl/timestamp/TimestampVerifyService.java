package bftsmart.communication.impl.timestamp;

/**
 * 时间戳校验接口
 *
 */
public interface TimestampVerifyService {

    /**
     * 返回时间校验是否完成
     *
     * @return
     */
    boolean timeVerifyCompleted();

    /**
     * 返回时间校验是否成功
     *
     * @return
     */
    boolean timeVerifySuccess();

    /**
     * 等待完成
     *
     * @param remoteId
     */
    void waitComplete(int remoteId);

    /**
     * 校验成功
     *
     * @param remoteId
     */
    void verifySuccess(int remoteId);

    /**
     * 校验失败
     *
     * @param remoteId
     */
    void verifyFail(int remoteId);

    /**
     * 返回校验时间戳
     *
     * @return
     */
    long verifyTimestamp();

    /**
     * 校验时间
     *
     * @param localTime
     * @param remoteId
     * @param remoteTime
     * @return
     */
    boolean verifyTime(long localTime, int remoteId, long remoteTime);
}
