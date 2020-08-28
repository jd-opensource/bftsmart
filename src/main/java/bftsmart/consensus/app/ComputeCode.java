package bftsmart.consensus.app;

public enum ComputeCode {
    /**
     * 成功
     *
     */
    SUCCESS((byte) 0x0),

    /**
     * 失败
     *
     */
    FAILURE((byte) 0x1);

    public final byte code;

    ComputeCode(byte code) {
        this.code = code;
    }

    public static ComputeCode valueOf(byte code) {
        for (ComputeCode value : values()) {
            if (value.code == code) {
                return value;
            }
        }
        throw new IllegalArgumentException("Unsupported precompute result code!");
    }

    public byte getCode() {
        return code;
    }
}
