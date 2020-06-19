/**
 * MogUtil is a dumping ground for random functions that seem like they might be useful in various places.
 */
public class MogUtil {
    /**
     * Lookup array of hex characters for the bytesToHex() function.
     */
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    /**
     * Convert bytes to hex characters.
     * Source: https://stackoverflow.com/a/9855338
     *
     * @param bytes Input bytes that should be converted to a hex representation.
     * @return The hex representation of the input bytes.
     */
    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }
}
