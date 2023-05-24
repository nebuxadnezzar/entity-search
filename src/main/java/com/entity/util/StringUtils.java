package com.entity.util;

//import java.io.*;
import java.util.*;
import java.security.MessageDigest;
//import java.security.NoSuchAlgorithmException;
//import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils {
    public static final String EMPTY = "";
    public static final String SPACE = " ";
    private static final String ZEROES = "0000000000000000000000000000000000000000000000000000000000000000";
    // private static final String[] NOES = new String[] { "0", "no" };
    private static final int PAD_LIMIT = 8192;

    // -------------------------------------------------------------------------
    public static boolean validateString(String pPattern, String pData) {
        if (isEmpty(pData)) {
            return false;
        }
        Pattern p = Pattern.compile(pPattern);
        return p.matcher(pData).matches();
    }

    // -------------------------------------------------------------------------
    public static boolean isNoValue(String val) {
        if (isBlank(val)) {
            return true;
        }
        String s = val.trim().toLowerCase();
        return "0".equals(s) || "no".equals(s);
    }

    // -------------------------------------------------------------------------
    public static String randomAlphaNumeric(int count, String alphabet) {
        if (isEmpty(alphabet)) {
            return "";
        }

        StringBuilder builder = new StringBuilder();
        while (count-- != 0) {
            int character = (int) (Math.random() * alphabet.length());
            builder.append(alphabet.charAt(character));
        }
        return builder.toString();
    }

    // -------------------------------------------------------------------------
    public static String defaultString(String str) {
        return str == null ? EMPTY : str;
    }

    // -------------------------------------------------------------------------
    public static boolean isEmpty(String str) {
        return (str == null || str.length() < 1);
    }/**/
    // -------------------------------------------------------------------------

    public static boolean isNotBlank(String str) {
        return !isBlank(str);
    }

    // -------------------------------------------------------------------------
    public static boolean isBlank(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if ((Character.isWhitespace(str.charAt(i)) == false)) {
                return false;
            }
        }
        return true;
    }

    // --------------------------------------------------------------------------
    // -- Counts how many times the substring appears in the larger String
    // -- taken from apache utils
    // --------------------------------------------------------------------------
    public static int countMatches(String str, String sub) {
        if (isEmpty(str) || isEmpty(sub)) {
            return 0;
        }
        int count = 0;
        int idx = 0;
        while ((idx = str.indexOf(sub, idx)) != -1) {
            count++;
            idx += sub.length();
        }
        return count;
    }

    // --------------------------------------------------------------------------
    public static int countMatches(byte[] ba, byte b) {
        int count = 0;

        if (ba == null) {
            return count;
        }

        for (int i = 0, k = ba.length; i < k; i++) {
            if (ba[i] == b) {
                ++count;
            }
        }
        return count;
    }

    // --------------------------------------------------------------------------
    // -- converts hex string to byte array
    // --------------------------------------------------------------------------
    public static byte[] toByteArray(String pHexString) {
        int len = pHexString.length();
        byte[] data = new byte[len / 2];

        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(pHexString.charAt(i), 16) << 4)
                    + Character.digit(pHexString.charAt(i + 1), 16));
        }
        return data;
    }

    // --------------------------------------------------------------------------
    public static String toHexString(byte[] pBuff) {
        StringBuilder lHexString = new StringBuilder();

        for (byte b : pBuff) {
            // System.out.print( ( char )b + " " );
            final String s = Integer.toHexString(0xFF & b);
            if (s.length() < 2) {
                lHexString.append('0');
            }
            lHexString.append(s);
        }
        return lHexString.toString();
    }

    // --------------------------------------------------------------------------
    public static String[] hexToString(String hex) {
        if (isBlank(hex)) {
            return new String[] { "", "" };
        }

        if (hex.length() % 2 != 0) {
            hex += "0";
        }

        StringBuilder sb = new StringBuilder();
        StringBuilder dec = new StringBuilder();

        for (int i = 0; i < hex.length() - 1; i += 2) {
            // grab the hex in pairs
            String output = hex.substring(i, (i + 2));
            // convert hex to decimal
            int decimal = Integer.parseInt(output, 16);
            // convert the decimal to character
            sb.append((char) decimal);

            dec.append(decimal);
        }
        // System.out.println("Decimal : " + dec.toString());

        return new String[] { sb.toString(), dec.toString() };
    }

    // --------------------------------------------------------------------------
    public static String join(Collection<?> pCollection, String pDelim) {
        if (pCollection == null || pCollection.isEmpty()) {
            return "";
        }

        StringBuilder buffer = new StringBuilder();

        Iterator<?> iter = pCollection.iterator();

        if (iter.hasNext()) {
            buffer.append(String.valueOf(iter.next()));

            while (iter.hasNext()) {
                buffer.append(pDelim);
                buffer.append(iter.next());
            }
        }
        return buffer.toString();
    }

    // ---------------------------------------------------------------------------
    public static String join(String[] pCollection, String pDelim) {
        if (pCollection == null || pCollection.length < 1) {
            return "";
        }

        int len = pCollection.length;

        if (len == 1) {
            return pCollection[0];
        }

        StringBuilder buffer = new StringBuilder(pCollection[0]);

        for (int i = 1; i < len; i++) {
            buffer.append(pDelim).append(pCollection[i]);
        }

        return buffer.toString();
    }

    // ---------------------------------------------------------------------------
    public static byte[] joinB(byte[][] pBytes, byte[] pDelim) {
        if (pBytes == null || pBytes.length < 1) {
            return new byte[] {};
        }

        int len = pBytes.length,
                nonEmptyCnt = 0,
                delimLen = pDelim != null ? pDelim.length : 0,
                idx = -1;

        if (len == 1) {
            return pBytes[0];
        }

        len = 0;

        // calculate length of total buffer and find first non empty array
        //
        for (int i = 0, k = pBytes.length; i < k; i++) {
            final byte[] b = pBytes[i];
            if (b != null && b.length > 0) {
                len += b.length;
                ++nonEmptyCnt;
                if (idx == -1) {
                    idx = i;
                }
            }
        }

        --nonEmptyCnt;

        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(len + delimLen * nonEmptyCnt);
        buffer.put(pBytes[idx++]);

        len = pBytes.length;

        for (int i = idx; i < len; i++) {
            final byte[] b = pBytes[i];
            if (pDelim != null) {
                buffer.put(pDelim);
            }
            if (b != null) {
                buffer.put(b);
            }
        }
        return buffer.array();
    }

    /*
     * slower
     *
     * public static byte [] joinB( byte [][] pBytes, byte [] pDelim )
     * {
     * if( pBytes == null || pBytes.length < 1 )
     * { return new byte[]{}; }
     *
     * int len = pBytes.length;
     *
     * if( len == 1 )
     * { return pBytes[0]; }
     *
     * ByteArrayBuilder buffer = new ByteArrayBuilder( pBytes[ 0 ] );
     *
     * for( int i = 1; i < len ; i++ )
     * {
     * if( pDelim != null )
     * { buffer.append( pDelim ); }
     * buffer.append( pBytes[ i ] );
     * }
     *
     * return buffer.toByteArray();
     * }
     */
    // ---------------------------------------------------------------------------
    public static byte[] joinB(Collection<byte[]> pCollection, byte[] pDelim)
            throws Exception {
        if (pCollection == null || pCollection.isEmpty()) {
            return new byte[0];
        }

        ByteArrayBuilder buffer = new ByteArrayBuilder();

        Iterator<?> iter = pCollection.iterator();

        if (iter.hasNext()) {
            buffer.append((byte[]) iter.next());

            while (iter.hasNext()) {
                buffer.append(pDelim);
                buffer.append((byte[]) iter.next());
            }
        }
        return buffer.toByteArray();
    }

    // ---------------------------------------------------------------------------
    public static byte[] joinB(String[] pCollection, byte[] pDelim)
            throws Exception {
        if (pCollection == null || pCollection.length < 1) {
            return new byte[] {};
        }

        int len = pCollection.length;

        if (len == 1) {
            return pCollection[0].getBytes("UTF-8");
        }

        ByteArrayBuilder buffer = new ByteArrayBuilder(pCollection[0]);

        for (int i = 1; i < len; i++) {
            buffer.append(pDelim).append(pCollection[i].getBytes("UTF-8"));
        }

        return buffer.toByteArray();
    }

    // -------------------------------------------------------------------------
    public static String abbreviate(String str, int lower, int upper, String appendToEnd) {
        if (str == null) {
            return null;
        }
        if (str.length() == 0) {
            return "";
        }
        if (lower > str.length()) {
            lower = str.length();
        }
        if ((upper == -1) || (upper > str.length())) {
            upper = str.length();
        }
        if (upper < lower) {
            upper = lower;
        }
        StringBuffer result = new StringBuffer();
        int index = str.indexOf(" ", lower);
        if (index == -1) {
            result.append(str.substring(0, upper));
            if (upper != str.length()) {
                result.append((appendToEnd == null ? "" : appendToEnd));
            }
        } else if (index > upper) {
            result.append(str.substring(0, upper));
            result.append((appendToEnd == null ? "" : appendToEnd));
        } else {
            result.append(str.substring(0, index));
            result.append((appendToEnd == null ? "" : appendToEnd));
        }
        return result.toString();
    }

    // -------------------------------------------------------------------------
    public static boolean containsAny(String str, String searchChars) {
        if (searchChars == null) {
            return false;
        }
        return containsAny(str, searchChars.toCharArray());
    }

    // -------------------------------------------------------------------------
    public static boolean containsAny(String str, char[] searchChars) {
        if (str == null || str.length() == 0 || searchChars == null || searchChars.length == 0) {
            return false;
        }
        for (int i = 0; i < str.length(); i++) {
            char ch = str.charAt(i);
            for (int j = 0; j < searchChars.length; j++) {
                if (searchChars[j] == ch) {
                    return true;
                }
            }
        }
        return false;
    }

    // -------------------------------------------------------------------------
    public static boolean containsNone(String str, char[] invalidChars) {
        if (str == null || invalidChars == null) {
            return true;
        }
        int strSize = str.length();
        int validSize = invalidChars.length;
        for (int i = 0; i < strSize; i++) {
            char ch = str.charAt(i);
            for (int j = 0; j < validSize; j++) {
                if (invalidChars[j] == ch) {
                    return false;
                }
            }
        }
        return true;
    }

    // -------------------------------------------------------------------------
    public static boolean containsNone(String str, String invalidChars) {
        if (str == null || invalidChars == null) {
            return true;
        }
        return containsNone(str, invalidChars.toCharArray());
    }

    // -------------------------------------------------------------------------
    /**
     * <p>
     * Checks if String contains a search String irrespective of case,
     * handling <code>null</code>. Case-insensitivity is defined as by
     * {@link String#equalsIgnoreCase(String)}.
     *
     * <p>
     * A <code>null</code> String will return <code>false</code>.
     * </p>
     *
     * <pre>
     * StringUtils.contains(null, *) = false
     * StringUtils.contains(*, null) = false
     * StringUtils.contains("", "") = true
     * StringUtils.contains("abc", "") = true
     * StringUtils.contains("abc", "a") = true
     * StringUtils.contains("abc", "z") = false
     * StringUtils.contains("abc", "A") = true
     * StringUtils.contains("abc", "Z") = false
     * </pre>
     *
     * @param str       the String to check, may be null
     * @param searchStr the String to find, may be null
     * @return true if the String contains the search String irrespective of
     *         case or false if not or <code>null</code> string input
     */
    public static boolean containsIgnoreCase(String str, String searchStr) {
        if (str == null || searchStr == null) {
            return false;
        }
        int len = searchStr.length();
        int max = str.length() - len;
        for (int i = 0; i <= max; i++) {
            if (str.regionMatches(true, i, searchStr, 0, len)) {
                return true;
            }
        }
        return false;
    }

    // -------------------------------------------------------------------------
    public static String replace(String text, String searchString, String replacement) {
        return replace(text, searchString, replacement, -1);
    }

    // -------------------------------------------------------------------------
    public static String replace(String text, String searchString, String replacement, int max) {
        if (isEmpty(text) || isEmpty(searchString) || replacement == null || max == 0) {
            return text;
        }
        int start = 0;
        int end = text.indexOf(searchString, start);
        if (end == -1) {
            return text;
        }
        int replLength = searchString.length();
        int increase = replacement.length() - replLength;
        increase = (increase < 0 ? 0 : increase);
        increase *= (max < 0 ? 16 : (max > 64 ? 64 : max));
        StringBuffer buf = new StringBuffer(text.length() + increase);
        while (end != -1) {
            buf.append(text.substring(start, end)).append(replacement);
            start = end + replLength;
            if (--max == 0) {
                break;
            }
            end = text.indexOf(searchString, start);
        }
        buf.append(text.substring(start));
        return buf.toString();
    }

    // -------------------------------------------------------------------------
    public static String normalize(String pText) {
        return normalize(pText, false, null);
    }

    // -------------------------------------------------------------------------
    public static String normalize(String pText, boolean pPreserveNewLines, String pNewLineReplacement) {
        if (StringUtils.isBlank(pText)) {
            return pText;
        }

        if (pPreserveNewLines && StringUtils.isNotBlank(pNewLineReplacement)) {
            return pText.replaceAll("\r", "")
                    .replaceAll("\n", pNewLineReplacement)
                    .replaceAll("\\s+", " ")
                    .replaceAll("(^\\s+|\\s+$)", "");
        }
        String lSpaceRegex = "(?s)\\s((?=\\s))"; // "(?s)\\s+", " "

        return pText.replaceAll(lSpaceRegex, "$1").replaceAll("(^\\s+|\\s+$)", "");
    }

    // -------------------------------------------------------------------------
    // -- strip punctuation
    // -------------------------------------------------------------------------
    public static String stripPunctuation(String pText) {
        return pText.replaceAll("\\p{Punct}", "");
    }

    // -------------------------------------------------------------------------
    // -- remove all but word chars and some puncuation
    // -------------------------------------------------------------------------
    public static String sterilize(String pText) {
        return sterilize(pText, false);
    }

    // -------------------------------------------------------------------------
    public static String sterilize(String pText, boolean pPreserveSpace) {
        if (StringUtils.isBlank(pText)) {
            return pText;
        }

        if (pPreserveSpace) {
            return pText.replaceAll("[^\\w\\s~/%.:,]", "")
                    .replaceAll("(^\\s+|\\s+$)", "");
        }

        return pText.replaceAll("[^\\w~/%.:,]", "");
    }

    // -------------------------------------------------------------------------
    public static String[] sterilize(String[] pText) {
        return sterilize(pText, false);
    }

    // -------------------------------------------------------------------------
    public static String[] sterilize(String[] pText, boolean pPreserveSpace) {
        if (pText == null) {
            return pText;
        }

        int len = pText.length;

        for (int i = 0; i < len; i++) {
            pText[i] = sterilize(pText[i], pPreserveSpace);
        }

        return pText;
    }

    // Character Tests
    // -----------------------------------------------------------------------
    /**
     * <p>
     * Checks if the String contains only unicode letters.
     * </p>
     *
     * <p>
     * <code>null</code> will return <code>false</code>.
     * An empty String ("") will return <code>true</code>.
     * </p>
     *
     * <pre>
     * StringUtils.isAlpha(null)   = false
     * StringUtils.isAlpha("")     = true
     * StringUtils.isAlpha("  ")   = false
     * StringUtils.isAlpha("abc")  = true
     * StringUtils.isAlpha("ab2c") = false
     * StringUtils.isAlpha("ab-c") = false
     * </pre>
     *
     * @param str the String to check, may be null
     * @return <code>true</code> if only contains letters, and is non-null
     */
    public static boolean isAlpha(String str) {
        if (str == null) {
            return false;
        }
        int sz = str.length();
        for (int i = 0; i < sz; i++) {
            if (Character.isLetter(str.charAt(i)) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * <p>
     * Checks if the String contains only unicode letters and
     * space (' ').
     * </p>
     *
     * <p>
     * <code>null</code> will return <code>false</code>
     * An empty String ("") will return <code>true</code>.
     * </p>
     *
     * <pre>
     * StringUtils.isAlphaSpace(null)   = false
     * StringUtils.isAlphaSpace("")     = true
     * StringUtils.isAlphaSpace("  ")   = true
     * StringUtils.isAlphaSpace("abc")  = true
     * StringUtils.isAlphaSpace("ab c") = true
     * StringUtils.isAlphaSpace("ab2c") = false
     * StringUtils.isAlphaSpace("ab-c") = false
     * </pre>
     *
     * @param str the String to check, may be null
     * @return <code>true</code> if only contains letters and space,
     *         and is non-null
     */
    public static boolean isAlphaSpace(String str) {
        if (str == null) {
            return false;
        }
        int sz = str.length();
        for (int i = 0; i < sz; i++) {
            if ((Character.isLetter(str.charAt(i)) == false) && (str.charAt(i) != ' ')) {
                return false;
            }
        }
        return true;
    }

    /**
     * <p>
     * Checks if the String contains only unicode letters or digits.
     * </p>
     *
     * <p>
     * <code>null</code> will return <code>false</code>.
     * An empty String ("") will return <code>true</code>.
     * </p>
     *
     * <pre>
     * StringUtils.isAlphanumeric(null)   = false
     * StringUtils.isAlphanumeric("")     = true
     * StringUtils.isAlphanumeric("  ")   = false
     * StringUtils.isAlphanumeric("abc")  = true
     * StringUtils.isAlphanumeric("ab c") = false
     * StringUtils.isAlphanumeric("ab2c") = true
     * StringUtils.isAlphanumeric("ab-c") = false
     * </pre>
     *
     * @param str the String to check, may be null
     * @return <code>true</code> if only contains letters or digits,
     *         and is non-null
     */
    public static boolean isAlphanumeric(String str) {
        if (str == null) {
            return false;
        }
        int sz = str.length();
        for (int i = 0; i < sz; i++) {
            if (Character.isLetterOrDigit(str.charAt(i)) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * <p>
     * Checks if the String contains only unicode letters, digits
     * or space (<code>' '</code>).
     * </p>
     *
     * <p>
     * <code>null</code> will return <code>false</code>.
     * An empty String ("") will return <code>true</code>.
     * </p>
     *
     * <pre>
     * StringUtils.isAlphanumeric(null)   = false
     * StringUtils.isAlphanumeric("")     = true
     * StringUtils.isAlphanumeric("  ")   = true
     * StringUtils.isAlphanumeric("abc")  = true
     * StringUtils.isAlphanumeric("ab c") = true
     * StringUtils.isAlphanumeric("ab2c") = true
     * StringUtils.isAlphanumeric("ab-c") = false
     * </pre>
     *
     * @param str the String to check, may be null
     * @return <code>true</code> if only contains letters, digits or space,
     *         and is non-null
     */
    public static boolean isAlphanumericSpace(String str) {
        if (str == null) {
            return false;
        }
        int sz = str.length();
        for (int i = 0; i < sz; i++) {
            if ((Character.isLetterOrDigit(str.charAt(i)) == false) && (str.charAt(i) != ' ')) {
                return false;
            }
        }
        return true;
    }

    /**
     * <p>
     * Checks if the string contains only ASCII printable characters.
     * </p>
     *
     * <p>
     * <code>null</code> will return <code>false</code>.
     * An empty String ("") will return <code>true</code>.
     * </p>
     *
     * <pre>
     * StringUtils.isAsciiPrintable(null)     = false
     * StringUtils.isAsciiPrintable("")       = true
     * StringUtils.isAsciiPrintable(" ")      = true
     * StringUtils.isAsciiPrintable("Ceki")   = true
     * StringUtils.isAsciiPrintable("ab2c")   = true
     * StringUtils.isAsciiPrintable("!ab-c~") = true
     * StringUtils.isAsciiPrintable(" ") = true
     * StringUtils.isAsciiPrintable("!") = true
     * StringUtils.isAsciiPrintable("~") = true
     * StringUtils.isAsciiPrintable("") = false
     * </pre>
     *
     * @param str the string to check, may be null
     * @return <code>true</code> if every character is in the range
     *         32 thru 126
     * @since 2.1
     */
    public static boolean isAsciiPrintable(String str) {
        if (str == null) {
            return false;
        }
        int sz = str.length();
        for (int i = 0; i < sz; i++) {
            if (/* CharUtils. */isAsciiPrintable(str.charAt(i)) == false) {
                return false;
            }
        }
        return true;
    }

    public static boolean isAsciiPrintable(char ch) {
        return ch >= 32 && ch < 127;
    }

    /**
     * <p>
     * Checks if the String contains only unicode digits.
     * A decimal point is not a unicode digit and returns false.
     * </p>
     *
     * <p>
     * <code>null</code> will return <code>false</code>.
     * An empty String ("") will return <code>false</code>.
     * </p>
     *
     * <pre>
     * StringUtils.isNumeric(null)   = false
     * StringUtils.isNumeric("")     = false
     * StringUtils.isNumeric("  ")   = false
     * StringUtils.isNumeric("123")  = true
     * StringUtils.isNumeric("12 3") = false
     * StringUtils.isNumeric("ab2c") = false
     * StringUtils.isNumeric("12-3") = false
     * StringUtils.isNumeric("12.3") = false
     * </pre>
     *
     * @param str the String to check, may be null
     * @return <code>true</code> if only contains digits, and is non-null
     */
    public static boolean isNumeric(String str) {
        if (isBlank(str)) {
            return false;
        }
        int sz = str.length();
        for (int i = 0; i < sz; i++) {
            if (Character.isDigit(str.charAt(i)) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * <p>
     * Checks if the String contains only unicode digits or space
     * (<code>' '</code>).
     * A decimal point is not a unicode digit and returns false.
     * </p>
     *
     * <p>
     * <code>null</code> will return <code>false</code>.
     * An empty String ("") will return <code>true</code>.
     * </p>
     *
     * <pre>
     * StringUtils.isNumeric(null)   = false
     * StringUtils.isNumeric("")     = true
     * StringUtils.isNumeric("  ")   = true
     * StringUtils.isNumeric("123")  = true
     * StringUtils.isNumeric("12 3") = true
     * StringUtils.isNumeric("ab2c") = false
     * StringUtils.isNumeric("12-3") = false
     * StringUtils.isNumeric("12.3") = false
     * </pre>
     *
     * @param str the String to check, may be null
     * @return <code>true</code> if only contains digits or space,
     *         and is non-null
     */
    public static boolean isNumericSpace(String str) {
        if (str == null) {
            return false;
        }
        int sz = str.length();
        for (int i = 0; i < sz; i++) {
            if ((Character.isDigit(str.charAt(i)) == false) && (str.charAt(i) != ' ')) {
                return false;
            }
        }
        return true;
    }

    /**
     * <p>
     * Checks if the String contains only whitespace.
     * </p>
     *
     * <p>
     * <code>null</code> will return <code>false</code>.
     * An empty String ("") will return <code>true</code>.
     * </p>
     *
     * <pre>
     * StringUtils.isWhitespace(null)   = false
     * StringUtils.isWhitespace("")     = true
     * StringUtils.isWhitespace("  ")   = true
     * StringUtils.isWhitespace("abc")  = false
     * StringUtils.isWhitespace("ab2c") = false
     * StringUtils.isWhitespace("ab-c") = false
     * </pre>
     *
     * @param str the String to check, may be null
     * @return <code>true</code> if only contains whitespace, and is non-null
     * @since 2.0
     */
    public static boolean isWhitespace(String str) {
        if (str == null) {
            return false;
        }
        int sz = str.length();
        for (int i = 0; i < sz; i++) {
            if ((Character.isWhitespace(str.charAt(i)) == false)) {
                return false;
            }
        }
        return true;
    }

    /**
     * <p>
     * Checks if the String contains only lowercase characters.
     * </p>
     *
     * <p>
     * <code>null</code> will return <code>false</code>.
     * An empty String ("") will return <code>false</code>.
     * </p>
     *
     * <pre>
     * StringUtils.isAllLowerCase(null)   = false
     * StringUtils.isAllLowerCase("")     = false
     * StringUtils.isAllLowerCase("  ")   = false
     * StringUtils.isAllLowerCase("abc")  = true
     * StringUtils.isAllLowerCase("abC") = false
     * </pre>
     *
     * @param str the String to check, may be null
     * @return <code>true</code> if only contains lowercase characters, and is
     *         non-null
     * @since 2.5
     */
    public static boolean isAllLowerCase(String str) {
        if (str == null || isEmpty(str)) {
            return false;
        }
        int sz = str.length();
        for (int i = 0; i < sz; i++) {
            if (Character.isLowerCase(str.charAt(i)) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * <p>
     * Checks if the String contains only uppercase characters.
     * </p>
     *
     * <p>
     * <code>null</code> will return <code>false</code>.
     * An empty String ("") will return <code>false</code>.
     * </p>
     *
     * <pre>
     * StringUtils.isAllUpperCase(null)   = false
     * StringUtils.isAllUpperCase("")     = false
     * StringUtils.isAllUpperCase("  ")   = false
     * StringUtils.isAllUpperCase("ABC")  = true
     * StringUtils.isAllUpperCase("aBC") = false
     * </pre>
     *
     * @param str the String to check, may be null
     * @return <code>true</code> if only contains uppercase characters, and is
     *         non-null
     * @since 2.5
     */
    public static boolean isAllUpperCase(String str) {
        if (str == null || isEmpty(str)) {
            return false;
        }
        int sz = str.length();
        for (int i = 0; i < sz; i++) {
            if (Character.isUpperCase(str.charAt(i)) == false) {
                return false;
            }
        }
        return true;
    }

    // -------------------------------------------------------------------------
    public static String hash(String msg, String algorithm) throws Exception {
        StringBuilder hexStr = new StringBuilder();

        MessageDigest md = MessageDigest.getInstance(algorithm);
        md.reset();
        byte[] buffer = msg.getBytes("UTF-8");
        md.update(buffer);
        byte[] digest = md.digest();

        for (int i = 0; i < digest.length; i++) {
            hexStr.append(Integer.toString((digest[i] & 0xff) + 0x100, 16).substring(1));
        }
        return hexStr.toString();
    }

    // -------------------------------------------------------------------------
    public static String tokensToVals(String pTxt, Object[] pParams, String pToken) {
        if (pParams == null || pParams.length == 0 || isBlank(pTxt)) {
            return pTxt;
        }

        int len = pParams.length;
        String lTok = isNotBlank(pToken) ? pToken : "?";

        // pTxt is padded at the end with extra space to make tokenizer not to miss
        // the last token
        //
        StringTokenizer lSt = new StringTokenizer(pTxt + " ", lTok);
        StringBuilder lSb = new StringBuilder();

        for (int i = 0; lSt.hasMoreTokens() && i < len; i++) {
            // System.out.printf( "PARAM %d %s\n", i, pParams[ i ] );

            String token = lSt.nextToken();
            lSb.append(token);

            if (lSt.hasMoreTokens()) {
                if (pParams[i].toString().equalsIgnoreCase("null")) {
                    lSb.append(pParams[i].toString().toLowerCase());
                } else {
                    lSb.append(pParams[i].toString());
                }

                // System.out.printf( "\tPARAM %d %s is appended\n", i, pParams[ i ] );
                // System.out.println( "\t" + lSb.toString() );
            }
        }

        if (lSt.hasMoreTokens()) {
            lSb.append(lSt.nextToken().trim());
        }

        return lSb.toString().replaceAll("'null'", "null");
    }

    public static boolean isNumber(String str) {
        if (StringUtils.isEmpty(str)) {
            return false;
        }
        char[] chars = str.toCharArray();
        int sz = chars.length;
        boolean hasExp = false;
        boolean hasDecPoint = false;
        boolean allowSigns = false;
        boolean foundDigit = false;
        // deal with any possible sign up front
        int start = (chars[0] == '-') ? 1 : 0;
        if (sz > start + 1) {
            if (chars[start] == '0' && chars[start + 1] == 'x') {
                int i = start + 2;
                if (i == sz) {
                    return false; // str == "0x"
                }
                // checking hex (it can't be anything else)
                for (; i < chars.length; i++) {
                    if ((chars[i] < '0' || chars[i] > '9')
                            && (chars[i] < 'a' || chars[i] > 'f')
                            && (chars[i] < 'A' || chars[i] > 'F')) {
                        return false;
                    }
                }
                return true;
            }
        }
        sz--; // don't want to loop to the last char, check it afterwords
              // for type qualifiers
        int i = start;
        // loop to the next to last char or to the last char if we need another digit to
        // make a valid number (e.g. chars[0..5] = "1234E")
        while (i < sz || (i < sz + 1 && allowSigns && !foundDigit)) {
            if (chars[i] >= '0' && chars[i] <= '9') {
                foundDigit = true;
                allowSigns = false;

            } else if (chars[i] == '.') {
                if (hasDecPoint || hasExp) {
                    // two decimal points or dec in exponent
                    return false;
                }
                hasDecPoint = true;
            } else if (chars[i] == 'e' || chars[i] == 'E') {
                // we've already taken care of hex.
                if (hasExp) {
                    // two E's
                    return false;
                }
                if (!foundDigit) {
                    return false;
                }
                hasExp = true;
                allowSigns = true;
            } else if (chars[i] == '+' || chars[i] == '-') {
                if (!allowSigns) {
                    return false;
                }
                allowSigns = false;
                foundDigit = false; // we need a digit after the E
            } else {
                return false;
            }
            i++;
        }
        if (i < chars.length) {
            if (chars[i] >= '0' && chars[i] <= '9') {
                // no type qualifier, OK
                return true;
            }
            if (chars[i] == 'e' || chars[i] == 'E') {
                // can't have an E at the last byte
                return false;
            }
            if (chars[i] == '.') {
                if (hasDecPoint || hasExp) {
                    // two decimal points or dec in exponent
                    return false;
                }
                // single trailing decimal point after non-exponent is ok
                return foundDigit;
            }
            if (!allowSigns
                    && (chars[i] == 'd'
                            || chars[i] == 'D'
                            || chars[i] == 'f'
                            || chars[i] == 'F')) {
                return foundDigit;
            }
            if (chars[i] == 'l'
                    || chars[i] == 'L') {
                // not allowing L with an exponent
                return foundDigit && !hasExp;
            }
            // last character is illegal
            return false;
        }
        // allowSigns is true iff the val ends in 'E'
        // found digit it to make sure weird stuff like '.' and '1E-' doesn't pass
        return !allowSigns && foundDigit;
    }

    // -------------------------------------------------------------------------
    public static String max(String... strings) {
        if (strings == null || strings.length < 1) {
            return "";
        }

        String t = strings[0];

        for (String s : strings) {
            if (s.compareTo(t) > 0) {
                t = s;
            }
        }
        return t;
    }

    // -------------------------------------------------------------------------
    public static String min(String... strings) {
        if (strings == null || strings.length < 1) {
            return "";
        }

        String t = strings[0];

        for (int i = 1, k = strings.length; i < k; i++) {
            String s = strings[i];
            if (s.compareTo(t) < 0) {
                t = s;
            }
        }
        return t;
    }

    // ------------------------------------------------------------------------
    public static List<String> splitIntoChanks(String txt, int chunkSize) {
        int sz = chunkSize;
        List<String> l = new ArrayList<String>();

        if (txt == null) {
            return l;
        }

        for (int i = 0, k = txt.length(); i < k; i += sz) {
            final int offset = i + sz < k ? i + sz : k;
            l.add(txt.substring(i, offset));
        }
        return l;
    }

    // ------------------------------------------------------------------------
    public static String[] createNgrams(String txt, int size) {
        int len = txt.length();

        if (size >= len) {
            return new String[] { txt };
        }

        String[] ngrams = new String[len];
        char[] chars = new char[len];
        StringBuilder sb = new StringBuilder();

        txt.getChars(0, len, chars, 0);

        for (int i = 0, k = len; i < k; i++) {
            for (int j = i, h = j + size; j < h; j++) {
                sb.append(chars[j % len]);
            }
            ngrams[i] = sb.toString();
            sb.delete(0, sb.length());
        }
        // for( String s : ngrams ){ System.out.println( "--> " + s ); }
        return ngrams;
    }

    // ------------------------------------------------------------------------
    public static String longToBinaryString(long val) {
        final String zeroes = ZEROES;
        final String s = Long.toBinaryString(val);
        return String.format("%s%s", zeroes.substring(0, 64 - s.length()), s);
    }

    // -------------------------------------------------------------------------
    public static String rightPad(final String str, final int size) {
        return rightPad(str, size, ' ');
    }

    // -------------------------------------------------------------------------
    public static String rightPad(final String str, final int size, final char padChar) {
        if (str == null) {
            return null;
        }
        final int pads = size - str.length();
        if (pads <= 0) {
            return str; // returns original String when possible
        }
        if (pads > PAD_LIMIT) {
            return rightPad(str, size, String.valueOf(padChar));
        }
        return str.concat(repeat(padChar, pads));
    }

    // -------------------------------------------------------------------------
    public static String rightPad(final String str, final int size, String padStr) {
        if (str == null) {
            return null;
        }
        if (isEmpty(padStr)) {
            padStr = SPACE;
        }
        final int padLen = padStr.length();
        final int strLen = str.length();
        final int pads = size - strLen;
        if (pads <= 0) {
            return str; // returns original String when possible
        }
        if (padLen == 1 && pads <= PAD_LIMIT) {
            return rightPad(str, size, padStr.charAt(0));
        }

        if (pads == padLen) {
            return str.concat(padStr);
        } else if (pads < padLen) {
            return str.concat(padStr.substring(0, pads));
        } else {
            final char[] padding = new char[pads];
            final char[] padChars = padStr.toCharArray();
            for (int i = 0; i < pads; i++) {
                padding[i] = padChars[i % padLen];
            }
            return str.concat(new String(padding));
        }
    }

    // -------------------------------------------------------------------------
    public static String repeat(final char ch, final int repeat) {
        if (repeat <= 0) {
            return EMPTY;
        }
        final char[] buf = new char[repeat];
        for (int i = repeat - 1; i >= 0; i--) {
            buf[i] = ch;
        }
        return new String(buf);
    }

    // -------------------------------------------------------------------------
    // -- taken from Lucene StringHelper.java
    // -------------------------------------------------------------------------
    @SuppressWarnings("fallthrough")
    public static int murmurhash3_x86_32(byte[] data, int offset, int len, int seed) {

        final int c1 = 0xcc9e2d51;
        final int c2 = 0x1b873593;

        int h1 = seed;
        int roundedEnd = offset + (len & 0xfffffffc); // round down to 4 byte block

        for (int i = offset; i < roundedEnd; i += 4) {
            // little endian load order
            int k1 = (data[i] & 0xff) | ((data[i + 1] & 0xff) << 8) | ((data[i + 2] & 0xff) << 16)
                    | (data[i + 3] << 24);
            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;

            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        // tail
        int k1 = 0;

        switch (len & 0x03) {
            case 3:
                k1 = (data[roundedEnd + 2] & 0xff) << 16;
                // fallthrough
            case 2:
                k1 |= (data[roundedEnd + 1] & 0xff) << 8;
                // fallthrough
            case 1:
                k1 |= (data[roundedEnd] & 0xff);
                k1 *= c1;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= c2;
                h1 ^= k1;
        }

        // finalization
        h1 ^= len;

        // fmix(h1);
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;

        return h1;
    }

    /**
     * murmur hash 2.0.
     *
     * @author Viliam Holub
     * @version 1.0.2
     *          Generates 64 bit hash from byte array of the given length and seed.
     *
     * @param data   byte array to hash
     * @param length length of the array to hash
     * @param seed   initial seed value
     * @return 64 bit hash of the given array
     */
    @SuppressWarnings("fallthrough")
    public static long hash64(final byte[] data, int length, int seed) {
        final long m = 0xc6a4a7935bd1e995L;
        final int r = 47;

        long h = (seed & 0xffffffffl) ^ (length * m);

        int length8 = length / 8;

        for (int i = 0; i < length8; i++) {
            final int i8 = i * 8;
            long k = ((long) data[i8 + 0] & 0xff) + (((long) data[i8 + 1] & 0xff) << 8)
                    + (((long) data[i8 + 2] & 0xff) << 16) + (((long) data[i8 + 3] & 0xff) << 24)
                    + (((long) data[i8 + 4] & 0xff) << 32) + (((long) data[i8 + 5] & 0xff) << 40)
                    + (((long) data[i8 + 6] & 0xff) << 48) + (((long) data[i8 + 7] & 0xff) << 56);

            k *= m;
            k ^= k >>> r;
            k *= m;

            h ^= k;
            h *= m;
        }

        switch (length % 8) {
            case 7:
                h ^= (long) (data[(length & ~7) + 6] & 0xff) << 48;
            case 6:
                h ^= (long) (data[(length & ~7) + 5] & 0xff) << 40;
            case 5:
                h ^= (long) (data[(length & ~7) + 4] & 0xff) << 32;
            case 4:
                h ^= (long) (data[(length & ~7) + 3] & 0xff) << 24;
            case 3:
                h ^= (long) (data[(length & ~7) + 2] & 0xff) << 16;
            case 2:
                h ^= (long) (data[(length & ~7) + 1] & 0xff) << 8;
            case 1:
                h ^= (long) (data[length & ~7] & 0xff);
                h *= m;
        }
        ;

        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;

        return h;
    }

    @SuppressWarnings("fallthrough")
    public static int hash32(final byte[] data, int length, int seed) {
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        final int m = 0x5bd1e995;
        final int r = 24;

        // Initialize the hash to a random value
        int h = seed ^ length;
        int length4 = length / 4;

        for (int i = 0; i < length4; i++) {
            final int i4 = i * 4;
            int k = (data[i4 + 0] & 0xff) + ((data[i4 + 1] & 0xff) << 8)
                    + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        // Handle the last few bytes of the input array
        switch (length % 4) {
            case 3:
                h ^= (data[(length & ~3) + 2] & 0xff) << 16;
            case 2:
                h ^= (data[(length & ~3) + 1] & 0xff) << 8;
            case 1:
                h ^= (data[length & ~3] & 0xff);
                h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }
}

/*
 * private static final Charset ISO_8859_1 = Charset.forName("ISO-8859-1");
 *
 * public static List<byte[]> splitMessageSept19(byte[] rawByte, String
 * tokenDelimiter) {
 * Pattern pattern = Pattern.compile(tokenDelimiter, Pattern.LITERAL);
 * String[] parts = pattern.split(new String(rawByte, ISO_8859_1), -1);
 * List<byte[]> ret = new ArrayList<byte[]>();
 * for (String part : parts)
 * ret.add(part.getBytes(ISO_8859_1));
 * return ret;
 * }
 *
 * public static void main(String... args) {
 * StringBuilder sb = new StringBuilder();
 * for(int i=0;i<256;i++)
 * sb.append((char) i);
 * byte[] bytes = sb.toString().getBytes(ISO_8859_1);
 * List<byte[]> list = splitMessageSept19(bytes, ",");
 * for (byte[] b : list)
 * System.out.println(Arrays.toString(b));
 * }
 */
