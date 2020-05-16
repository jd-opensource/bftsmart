package bftsmart.consensus.app;

public enum ErrorCode {
    PRECOMPUTE_SUCC((byte) 0x0),
    PRECOMPUTE_FAIL((byte) 0x1);

    public final byte CODE;

    private ErrorCode(byte code) {
        this.CODE = code;
    }

    public static ErrorCode valueOf(byte code) {
        for (ErrorCode value : values()) {
            if (value.CODE == code) {
                return value;
            }
        }
        throw new IllegalArgumentException("Unsupported precompute result code!");
    }
}
