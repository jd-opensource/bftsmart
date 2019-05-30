package bftsmart.tom.util;

public class BytesUtils {

    private BytesUtils() {

    }

    public static byte[] getBytes(String str) {
        try {

            return str.getBytes("utf-8");

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
