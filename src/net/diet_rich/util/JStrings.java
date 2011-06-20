// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util;

/**
 * static string utility methods. adapted version of 
 * http://jabak.svn.sourceforge.net/viewvc/jabak/util/trunk/src/jut/lang/Strings.java
 */
public final class JStrings {

    /** private default constructor for utility class with exclusively static methods. */
    private JStrings() { throw new UnsupportedOperationException(); }

    /**
     * table of the 16 characters used in hex strings, used for hex conversions.
     */
    public static final char[] HEX_CHARS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A',
            'B', 'C', 'D', 'E', 'F' };

    /**
     * table of integer values of the 16 characters used in hex strings, used for hex conversions.
     * table cells that do not represent a hex value are initialized with -0x100.
     */
    // note - this table is initialized in the static initializer
    public static final int[] CHAR_HEX;

    // static initializer for the CHAR_HEX table
    static {
        CHAR_HEX = new int['g']; // to map characters up to 'f'
        for (char c = 0; c < 'g'; c++) CHAR_HEX[c] = -0x100; // invalidate all entries
        for (int i = 0; i <= 9; i++) CHAR_HEX[i + '0'] = i; // re-validate valid entries
        for (int i = 0; i < 6; i++) {
            CHAR_HEX[i + 'A'] = i + 10;
            CHAR_HEX[i + 'a'] = i + 10;
        }
    }

    /**
     * returns the corresponding upper-case hex string for a byte array.
     * <p>
     * this method is performance optimized.
     * 
     * @param bytes - the byte array to create the hex string for.
     * 
     * @return the corresponding hex string for a byte array.
     */
    public static String bytes2Hex(final byte[] bytes) {
        char[] result = new char[bytes.length * 2];
        int index = 0;
        for (byte b : bytes) {
            result[index++] = HEX_CHARS[(b & 0xf0) >> 4];
            result[index++] = HEX_CHARS[b & 0x0f];
        }
        return new String(result);
    }

    /**
     * returns the corresponding upper-case hex string for the first bytes of a byte array.
     * <p>
     * this method is performance optimized.
     * 
     * @param bytes - the byte array to create the hex string from.
     * @param len - the number of bytes to create the hex string for.
     * 
     * @return the corresponding hex string for a byte array.
     */
    public static String bytes2Hex(final byte[] bytes, final int len) {
        if (len < 0 || len > bytes.length)
            throw new IllegalStateException("illegal part length " + len + " for array length "
                    + bytes.length);
        char[] result = new char[len * 2];
        for (int cindex = 0, bindex = 0; bindex < len; bindex++) {
            byte byt = bytes[bindex];
            result[cindex++] = HEX_CHARS[(byt & 0xf0) >> 4];
            result[cindex++] = HEX_CHARS[byt & 0x0f];
        }
        return new String(result);
    }
    
    /**
     * returns the corresponding byte array for a hex string. this method accepts both upper case and
     * lower case hex strings. for invalid input strings, an {@link IllegalArgumentException} is
     * thrown.
     * <p>
     * this method is performance optimized.
     * 
     * @param string - the hex string to create a byte array for.
     * 
     * @return the corresponding byte array for a hex string.
     */
    public static byte[] hex2Bytes(final String string) {
        try {
            int len = string.length();
            if ((len & 1) == 1) throw new IllegalArgumentException("odd hex string length not allowed.");
            byte[] result = new byte[len / 2];
            for (int charIndex = 0, byteIndex = 0; charIndex < len;) {
                // although the following line looks more optimized:
                // result[j++] = (byte) (CHAR_HEX[string.charAt(i++)] * 16 + CHAR_HEX[string.charAt(i++)]);
                // on my system with Sun's Java 6, the implementation
                // below showed 10% better performance while at the same
                // time checking for illegal hex strings.
                int val = CHAR_HEX[string.charAt(charIndex++)] * 16 + CHAR_HEX[string.charAt(charIndex++)];
                if (val < 0) throw new IllegalArgumentException("not a hex string: " + string);
                result[byteIndex++] = (byte) val;
            }
            return result;
        } catch (IndexOutOfBoundsException e) {
            // PMD: the details of the IndexOutOfBoundsException are not relevant
            throw new IllegalArgumentException("not a hex string: " + string); // NOPMD by die on 15.09.09 09:04
        }
    }

}