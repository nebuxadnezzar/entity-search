package com.entity.util;

import java.util.*;
import java.text.*;
import java.nio.*;
import java.nio.file.*;
import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.AbstractMap.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class SimpleUtils {
    public static final int LOAD_PROPS = 1; // key=value file
    public static final int LOAD_XML = 2; // <entry key="x"> value </entry> file
    public static final int LOAD_UNK = 3; // free text file - 2b implemented

    // execution status
    //
    public static final String XSTATUS_AJAX_TEMPLATE = "{\"xstatus\":\"%s\", \"message\":\"%s\"}";
    public static final String EMAIL_REGEX = "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@" +
            "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";
    public static final String PASSWORD_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_!%~";

    private static final String[] DATE_FORMATS = {
            "yyyy/MM/dd",
            "yyyy-MM-dd",
            "MM/dd/yyyy HH:mm:ss",
            "yyyyMMdd HH:mm:ss",
            "yyyy-MM-dd HH:mm:ss",

            "MM/dd/yyyy HH:mm",
            "yyyyMMdd HH:mm",
            "yyyy-MM-dd HH:mm",

            "MM/dd/yyyy",
            "yyyyMMdd"
    };

    public static final long BASE_LINE_MILLIS = convertStringToDate("2017-01-01").getTime();

    // -------------------------------------------------------------------------
    // -- DATA CONVERSION SECTION
    // -------------------------------------------------------------------------
    public static java.sql.Date convertDateToSqlDate(java.util.Date pVal) {
        if (pVal == null) {
            return (java.sql.Date) null;
        }

        java.sql.Date dt = new java.sql.Date(pVal.getTime());
        dt.setTime(pVal.getTime());
        return dt;
    }

    // -------------------------------------------------------------------------
    public static java.util.Date convertStringToDate(String pVal, String pFormat) {
        java.util.Date d = null;

        if (StringUtils.isBlank(pVal)) {
            return d;
        }

        try {
            d = new SimpleDateFormat(pFormat).parse(pVal);
        } catch (Exception e) {
        }

        return d;
    }

    // -------------------------------------------------------------------------
    public static java.util.Date convertStringToDate(String pVal) {
        java.util.Date d = null;

        if (StringUtils.isBlank(pVal)) {
            return d;
        }

        for (String s : DATE_FORMATS) {
            if ((d = convertStringToDate(pVal, s)) != null) {
                break;
            }
        }

        if (d == null) {
            throw new RuntimeException("Invalid date passed: " + pVal);
        }

        return d;
    }

    // -------------------------------------------------------------------------
    public static Map<String, String> convertToSingleValueMap(Map<String, String[]> pMap) {
        Map<String, String> m = new HashMap<String, String>();

        if (SimpleUtils.isEmpty(pMap)) {
            return m;
        }
        for (Map.Entry<String, String[]> e : pMap.entrySet()) {
            final String s = String.join(",", e.getValue());
            if (StringUtils.isNotBlank(s)) {
                m.put(e.getKey(), s);
            }
        }
        return m;
    }

    // --------------------------------------------------------------------------
    public static String mapToParamString(Map<String, String> m) {
        StringBuilder sb = new StringBuilder();

        if (isEmpty(m)) {
            return sb.toString();
        }
        int cnt = 0;
        for (Map.Entry<String, String> e : m.entrySet()) {
            sb.append(e.getKey()).append('=').append(e.getValue());
            if (cnt++ + 1 < m.size())
                sb.append(' ');
        }
        return sb.toString();
    }

    // --------------------------------------------------------------------------
    public static String dateToString(java.util.Date pDate, String pFormat) {
        DateFormat lFormat = new SimpleDateFormat(pFormat);
        String lDateStr = lFormat.format(pDate);
        // System.out.println( "DATE STRING : " + lDateStr );
        return lDateStr;
    }

    // -------------------------------------------------------------------------
    public static byte[] longToBytes(long pVal) {
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(pVal).array();
    }

    // -------------------------------------------------------------------------
    public static long bytesToLong(byte[] pVal) {
        return ByteBuffer.wrap(pVal).order(ByteOrder.BIG_ENDIAN).getLong();
    }

    // -------------------------------------------------------------------------
    public static byte[] doubleToBytes(double pVal) {
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putDouble(pVal).array();
    }

    public static double bytesToDouble(byte[] pVal) {
        return ByteBuffer.wrap(pVal).order(ByteOrder.BIG_ENDIAN).getDouble();
    }

    // ------------------------------------------------------------------------
    public static short[] longToShorts(long val) {
        long mask = 0x00000000ffffL;
        short[] s = new short[4];

        s[3] = (short) (val & mask);
        s[2] = (short) ((val >>> 16) & mask);
        s[1] = (short) ((val >>> 32) & mask);
        s[0] = (short) ((val >>> 48) & mask);

        return s;
    }

    // -------------------------------------------------------------------------
    // -- MAP and PROPERTIES SECTION
    // -------------------------------------------------------------------------
    public static int getIntProperty(Properties pPropertyMap, String pKey) {
        return Integer.valueOf(getStringProperty(pPropertyMap, pKey, true)).intValue();
    }

    // -------------------------------------------------------------------------
    public static String getStringProperty(Properties pPropertyMap, String pKey) {
        return getStringProperty(pPropertyMap, pKey, false);
    }

    // -------------------------------------------------------------------------
    public static String getStringProperty(Properties pPropertyMap,
            String pKey,
            boolean pNormalize) {
        String lVal = pPropertyMap.getProperty(pKey);

        return (pNormalize ? lVal.replaceAll("(\\n|\\r)+", " ")
                .replaceAll("\\s+", " ")
                .replaceAll("(^\\s+|\\s+$)", "") : lVal);
    }

    // -------------------------------------------------------------------------
    public static Properties getXmlMap(String pResourcePath) {
        return getPropertiesMap(pResourcePath, LOAD_XML);
    }

    // -------------------------------------------------------------------------
    public static Properties getPropertiesMap(String pResourcePath) {
        return getPropertiesMap(pResourcePath, LOAD_PROPS);
    }

    // -------------------------------------------------------------------------
    public static Properties getPropertiesMap(String pResourcePath, int pPropertiesType) {
        Properties lProps = new Properties();

        try {
            InputStream in = pathToInStream(pResourcePath);
            // System.out.println( "!!! RESOURCE PATH " + pResourcePath + " !!!\n\n" );
            // System.out.println( "!!! INPUT STREAM " + ( in == null ? "<null> " : in ) + "
            // !!!\n\n" );

            switch (pPropertiesType) {
                case LOAD_XML: {
                    lProps.loadFromXML(in);
                }
                    break;
                case LOAD_PROPS: {
                    lProps.load(in);
                }
                    break;
                default: {
                    lProps.load(in);
                }
            }
        } catch (Exception e) {
            System.err.println(e);
            e.printStackTrace();
        }
        return lProps;
    }

    // --------------------------------------------------------------------------
    // -- reflection and object related functionality
    // --------------------------------------------------------------------------
    public static boolean isArray(Object o) {
        return isArray(o.getClass());
    }

    // --------------------------------------------------------------------------
    public static boolean isArray(Class<?> cls) {
        return cls.isArray();
    }

    // -------------------------------------------------------------------------
    public final static Object getObjectForClass(String pClassName)
            throws Exception {
        Object lObject = null;

        if (pClassName != null) {
            Class<?> lCls = Class.forName(pClassName);
            // lObject = lCls.newInstance();
            lObject = lCls.getDeclaredConstructor().newInstance();
        }
        return lObject;
    }

    // -------------------------------------------------------------------------
    public static void setObjectProperty(Object pBean, String pPropName, Object pPropVal)
            throws Exception {
        Class<?> lCls = pBean.getClass();
        java.lang.reflect.Field fld = lCls.getDeclaredField(pPropName);
        fld.setAccessible(true);
        fld.set(pBean, pPropVal);
    }

    // -------------------------------------------------------------------------
    public static Object defaultObjVal(Object obj) {
        if (obj == null) {
            throw new IllegalArgumentException("defaultObjVal requires non-empty argument");
        }

        if (obj instanceof Double) {
            return Double.valueOf(0.0);
        }
        if (obj instanceof Integer) {
            return Integer.valueOf(0);
        }
        if (obj instanceof Long) {
            return Long.valueOf(0);
        }
        if (obj instanceof String) {
            return "";
        }

        throw new RuntimeException("defaultObjVal: unsupported object type " + obj.toString());
    }

    // --------------------------------------------------------------------------
    // --------------------------------------------------------------------------
    // -- generic methods to output object's content
    // --------------------------------------------------------------------------
    public static String toString(Object pObj) {
        String fullClassName = Thread.currentThread().getStackTrace()[2].getClassName();
        String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
        int lineNumber = Thread.currentThread().getStackTrace()[2].getLineNumber();
        System.out.printf("%s.%s() : line %d\n", fullClassName, methodName, lineNumber);

        if (pObj == null) {
            return "[null]";
        }

        return toString(pObj, pObj.getClass());
    }

    // --------------------------------------------------------------------------
    public static String toString(Object pObj, Class<?> pClass) {
        if (pObj == null || pClass == null) {
            return "[null]";
        }
        try {
            Class<?> cls = pClass;
            StringBuilder result = new StringBuilder();

            result.append("[" + cls.getName() + "\n\n");
            java.lang.reflect.Field fieldlist[] = cls.getDeclaredFields();

            for (int i = 0; i < fieldlist.length; i++) {
                java.lang.reflect.Field fld = fieldlist[i];
                result.append(" field [\n");
                result.append("\t name = " + fld.getName() + "\n");
                result.append("\t type = " + fld.getType().toString() + "\n");
                int mod = fld.getModifiers();
                result.append("\t modifiers = " + java.lang.reflect.Modifier.toString(mod) + "\n");
                fld.setAccessible(true);
                result.append("\t value = " + fld.get(pObj) + "\n");
                result.append("       ]\n");
            }

            java.lang.reflect.Method[] allMethods = cls.getDeclaredMethods();
            for (java.lang.reflect.Method m : allMethods) {
                try {
                    result.append("\t method = " + m.getName() + ":" +
                            ((Class<?>) m.getGenericReturnType()).getName() + "\n");
                } catch (Exception e) {
                    result.append("\t method = " + m.getName() + "\n");
                }
            }
            result.append("]" + "\n");
            return result.toString();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return "";
    }

    // --------------------------------------------------------------------------
    public static boolean isNotEmpty(Object pObj) {
        return !isEmpty(pObj);
    }

    // --------------------------------------------------------------------------
    public static boolean isEmpty(Object pObj) {
        if (pObj == null) {
            return true;
        }

        if (pObj.getClass().isArray()) {
            return java.lang.reflect.Array.getLength(pObj) < 1;
        }

        if (pObj instanceof String) {
            return ((String) pObj).length() < 1;
        }

        if (pObj instanceof Collection) {
            return ((Collection) pObj).isEmpty();
        }

        if (pObj instanceof Map) {
            return ((Map) pObj).isEmpty();
        } else {
            throw new RuntimeException(
                    "Unsupported object type passed to SimpleUtils.isEmpty()\n" +
                            "Legal types are String, String[], Collection and Map");
        }
    }

    // --------------------------------------------------------------------------
    // -- IO and FILES
    // --------------------------------------------------------------------------
    public static InputStream pathToInStream(String pPath) throws Exception {
        // try resource first
        //
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(pPath);
        // System.out.println( pPath );
        // System.out.println( in );
        if (in == null && StringUtils.isNotBlank(pPath) && fileExists(pPath)) {
            in = new FileInputStream(pPath);
        }
        return in;
    }

    // --------------------------------------------------------------------------
    public static OutputStream pathToOutStream(String pPath, boolean pAppend) throws IOException {
        File outputFile = new File(pPath);

        if (outputFile.getParentFile() != null) {
            outputFile.getParentFile().mkdirs();
        }

        return new FileOutputStream(pPath, pAppend);
    }

    // --------------------------------------------------------------------------
    public static boolean makeDir(String dirname) {
        if (((new File(dirname)).exists()) ||
                ((new File(dirname)).mkdirs())) {
            return true;
        } else {
            System.err.println("failed to create " + dirname);
            return false;
        }
    }

    // --------------------------------------------------------------------------
    public static boolean fileExists(String pFilePath) {
        return ((new File(pFilePath)).exists());
    }

    // --------------------------------------------------------------------------
    public static boolean writeToFile(String pFilePath, String pText) {
        boolean retval = false;
        try {
            PrintWriter lPw = new PrintWriter(new FileWriter(pFilePath, true));
            lPw.print(pText);
            lPw.close();
            retval = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retval;
    }

    // -------------------------------------------------------------------------
    public static Map<String, Map<String, String>> readIniFile(String resourcePath) {
        Map<String, Map<String, String>> map = null;

        try {
            InputStream in = pathToInStream(resourcePath);
            map = readIniFile(in);
        } catch (Exception e) {
            System.out.println("FAILED TO READ " + resourcePath + " " + e);
            e.printStackTrace();
        }
        return map;
    }

    // -------------------------------------------------------------------------
    public static Map<String, Map<String, String>> readIniFile(InputStream stream) throws Exception {
        String buff = slurpFile(stream);

        if (buff == null) {
            return null;
        }

        Pattern pattern = Pattern.compile("\\[(.*)\\]");
        Map<String, Map<String, String>> imap = new HashMap<String, Map<String, String>>();
        String sep = "\n", // System.getProperty( "line.separator" ),
                sec = null;
        String[] ss = buff.split(sep, -1);

        for (String z : ss) {
            String s = z.replaceAll("\\r", "");
            // System.out.println( "-> " + s );

            Matcher match = pattern.matcher(s);

            if (match.find() && StringUtils.isNotBlank(match.group(1))) {
                sec = match.group(1).trim();

                imap.put(sec, new LinkedHashMap<String, String>());
                // System.out.println( "SECTION " + sec );
            } else

            if (s.startsWith(";")) {
                continue;
            } else

            if (sec != null && s.contains("=")) {
                String[] t = s.split("=", 2);
                imap.get(sec).put(t[0], t[1]);
            }
        }
        return imap;
    }

    // --------------------------------------------------------------------------
    public static String slurpFile(String path) throws IOException {
        return slurpFile(new FileInputStream(new File(path)));
    }

    // --------------------------------------------------------------------------
    public static String slurpFile(InputStream stream) throws IOException {
        StringBuilder result = new StringBuilder();
        try {
            int ch = 0;

            while ((ch = stream.read()) != -1) {
                result.append((char) ch);
            }
        } finally {
            try {
                stream.close();
            } catch (Exception e) {
            }
        }

        return result.toString();
    }

    // --------------------------------------------------------------------------
    public static byte[] getFileBytes(String path) throws IOException {
        return Files.readAllBytes(Paths.get(path));
    }

    // --------------------------------------------------------------------------
    public static void deleteFile(String pDirPath, String pNamePattern, long intervalMillis) {
        String[] lFileList = getFileList(pDirPath, pNamePattern, true);
        long currentMillis = System.currentTimeMillis();
        for (String s : lFileList) {
            File f = new File(pDirPath + System.getProperty("file.separator") + s);
            // System.out.printf( "FILE %s millis %d\n",s, currentMillis - f.lastModified()
            // );
            if (currentMillis - f.lastModified() > intervalMillis) {
                f.delete();
            }
        }
    }

    // ---------------------------------------------------------------------------
    public static String[] getFileList(String pDirPath,
            final String pPattern,
            final boolean pCaseInsensitive) {
        File lDir = new File(pDirPath);
        String[] lFileList = null;

        if (lDir != null && lDir.exists() && lDir.isDirectory()) {
            FilenameFilter lFilter = new FilenameFilter() {
                public boolean accept(File f, String s) {
                    Pattern pattern = Pattern.compile(pCaseInsensitive ? pPattern.toLowerCase() : pPattern);

                    String name = pCaseInsensitive ? s.toLowerCase() : s;

                    if (pattern.matcher(name).find()) {
                        // System.out.printf( "--> %s\n", name );
                        return true;
                    }
                    return false;
                }
            };

            lFileList = lDir.list(lFilter);
            // System.out.println( pDirPath + " exists" );
        } else {// System.out.println( pDirPath + " does not exist" );
            lFileList = new String[0];
        }

        return lFileList;
    }

    // --------------------------------------------------------------------------
    // -- PARSING
    // --------------------------------------------------------------------------
    public static Map<String, String[]> parseQueryString(String q) {
        Map<String, Set<String>> m = new LinkedHashMap<String, Set<String>>();
        Map<String, String[]> z = new LinkedHashMap<String, String[]>();

        if (isEmpty(q)) {
            return z;
        }

        for (String s : q.split("&")) {
            SimpleImmutableEntry<String, String> e = splitQueryParameter(s);
            final String key = e.getKey();
            if (!m.containsKey(key)) {
                m.put(key, new HashSet<String>());
            }

            m.get(key).add(e.getValue());
        }

        for (Map.Entry<String, Set<String>> e : m.entrySet()) {
            z.put(e.getKey(), e.getValue().toArray(new String[e.getValue().size()]));
        }

        return z;
    }

    // -------------------------------------------------------------------------
    // -- taken straight from StackOverflow
    // -------------------------------------------------------------------------
    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return (o1.getValue()).compareTo(o2.getValue());
            }
        });

        Map<K, V> result = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    // --------------------------------------------------------------------------
    public static SimpleImmutableEntry<String, String> splitQueryParameter(String it) {
        final int idx = it.indexOf("=");
        final String key = idx > 0 ? it.substring(0, idx) : it;
        final String value = idx > 0 && it.length() > idx + 1 ? it.substring(idx + 1) : null;
        return new SimpleImmutableEntry<>(key, value);
    }

    // --------------------------------------------------------------------------
    // -- MATH
    // --------------------------------------------------------------------------
    public static long addExact(long x, long y) {
        long r = x + y;
        // HD 2-12 Overflow iff both arguments have the opposite sign of the result
        if (((x ^ r) & (y ^ r)) < 0) {
            throw new ArithmeticException("long overflow");
        }
        return r;
    }

    // --------------------------------------------------------------------------
    // -- returns either index of found string or suggested position for the string
    // -- if suggest_position was true or -1 if string not found
    // --------------------------------------------------------------------------
    /*
     * public static int bsearchStr( String [] ary, String val, boolean
     * suggest_position )
     * {
     * int h = ary.length, l = -1, m;
     *
     * while( h - l > 1 )
     * {
     * if( ary[ m = h + l >>> 1 ].compareTo( val ) < 0 ){ l = m; }
     * else{ h = m; }
     * }
     * return ( h < 0 || h >= ary.length || ary[ h ].compareTo( val ) != 0 ) ?
     * suggest_position ? h : -1 : h;
     * }
     */
    // --------------------------------------------------------------------------
    public static <T> int bsearch(Comparable<T>[] ary, T val, boolean suggest_position) {
        int h = ary.length, l = -1, m;

        int retval = -1;

        while (h - l > 1 && retval != 0) {
            retval = ary[m = h + l >>> 1].compareTo(val);
            if (retval < 0) {
                l = m;
            } else {
                h = m;
            }
        }
        return (h < 0 || h >= ary.length || retval != 0) ? suggest_position ? h : -1 : h;
    }

    // --------------------------------------------------------------------------
    public static <T> int bsearch(T[] ary, T val, Comparator<T> cmp) {
        int h = ary.length, l = -1, m;

        int retval = -1;

        while (h - l > 1 && retval != 0) {
            retval = cmp.compare(ary[m = h + l >>> 1], val);
            if (retval < 0) {
                l = m;
            } else {
                h = m;
            }
        }
        return (h < 0 || h >= ary.length || retval != 0) ? -1 * h - 1 : h;
    }

    // --------------------------------------------------------------------------
    public static <T> void removeDups(List<T> pList) {
        Set<T> hs = new LinkedHashSet<T>();
        hs.addAll(pList);
        pList.clear();
        pList.addAll(hs);
    }

    // --------------------------------------------------------------------------
    // -- debug functionality
    // --------------------------------------------------------------------------
    public static int getRecursionLevel(String pMethodName) {
        StackTraceElement[] lElements = Thread.currentThread().getStackTrace();
        int cnt = 0;

        for (int i = 2; i < lElements.length; i++) {
            String s = lElements[i].toString();
            // System.out.println( "-> " + s );
            if (s.contains(pMethodName)) {
                ++cnt;
            }
        }
        return cnt;
    }

    // --------------------------------------------------------------------------
    public static void printStackTrace() {
        StackTraceElement[] lElements = Thread.currentThread().getStackTrace();

        for (int i = 2; i < lElements.length; i++) {
            System.out.println("-> " + lElements[i]);
        }
    }

    // --------------------------------------------------------------------------
    public static void printStackTrace(int pNumberOfElements) {
        StackTraceElement[] lElements = Thread.currentThread().getStackTrace();
        int lLimit = pNumberOfElements > lElements.length ||
                pNumberOfElements < 1 ? lElements.length : pNumberOfElements;

        for (int i = 2; i < lLimit + 2; i++) {
            System.out.println("-> " + lElements[i]);
        }
    }

    // --------------------------------------------------------------------------
    public static String errorStackTraceToString(Throwable e, String filter) {
        StringBuilder sb = new StringBuilder();

        for (StackTraceElement element : e.getStackTrace()) {
            String s = element.toString();
            // System.out.println( "-> " + s );
            if (StringUtils.isBlank(filter) || s.contains(filter)) {
                sb.append("\n-> " + s);
            }
        }
        return sb.toString();
    }

    // ---------------------------------------------------------------------------
    public static void printParameterMap(Map<String, String[]> pParamMap) {
        for (Map.Entry<String, String[]> entry : pParamMap.entrySet()) {
            System.out.println("Param key = " + entry.getKey());

            for (String s : entry.getValue()) {
                System.out.println("\tParam val = " + s);
            }
        }
    }

    // ---------------------------------------------------------------------------
    public static void printMap(Map<String, String> pParamMap) {
        for (Map.Entry<String, String> entry : pParamMap.entrySet()) {
            System.out.printf("K = %s\t\tV = %s\n", entry.getKey(), entry.getValue());
        }
    }

    // ---------------------------------------------------------------------------
    public static String sprintfMap(Map<String, String> pParamMap) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : pParamMap.entrySet()) {
            sb.append(String.format("\tK = %s\t\tV = %s\n", entry.getKey(), entry.getValue()));
        }
        return sb.toString();
    }

    // --------------------------------------------------------------------------
    // -- used to stdout string content in bytes do debug offencive chars
    // --------------------------------------------------------------------------
    public static void printStringBytes(String pVal) {
        printStringBytes(pVal.getBytes());
    }

    public static void printStringBytes(byte[] pVal) {
        byte[] bytes = pVal;
        System.out.println("\n\nSTRING BYTES\n!!!\n");
        for (byte b : bytes) {
            System.out.printf("%c-%d ", (char) b, (int) b);
        }
        System.out.println("!!!\n");
    }

    // ----------------------------------------------------------------------------
    public static void printEnv() {

        System.out.println(System.getenv());

        Properties pr = System.getProperties();
        TreeSet<String> propKeys = new java.util.TreeSet(pr.keySet());
        for (Iterator it = propKeys.iterator(); it.hasNext();) {
            String key = (String) it.next();
            if (!key.equals("line.separator")) {
                System.out.println("--> " + key + "=" + pr.get(key));
            } else {
                System.out.println("--> " + key + "=" + toHexString(((String) pr.get(key)).getBytes()));
            }
        }
    }

    // ----------------------------------------------------------------------------
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

    // ----------------------------------------------------------------------------
    public static class DaemonThreadFactory implements ThreadFactory {
        static final AtomicInteger poolNumber = new AtomicInteger(1);
        final AtomicInteger threadNumber = new AtomicInteger(1);
        final String namePrefix;

        public DaemonThreadFactory() {
            namePrefix = "pool-" + poolNumber.getAndIncrement() + "-daemon-thread-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(Thread.currentThread().getThreadGroup(), r,
                    namePrefix + threadNumber.getAndIncrement());
            t.setDaemon(true);
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            // System.err.println( "THREAD " + t.getName() );
            return t;
        }
    }
}
