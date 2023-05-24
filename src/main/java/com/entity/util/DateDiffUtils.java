package com.entity.util;

import java.util.*;

public class DateDiffUtils {
    public static final int MILLIS = 1000;
    public static final int SECONDS = 60;
    public static final int MINUTES = 60;
    public static final int HOURS = 24;

    public static final int DD = 0;
    public static final int HH = 1;
    public static final int NN = 2;
    public static final int SS = 3;
    public static final int MS = 4;

    private static final Set<Integer> _types = new HashSet<Integer>(
            Arrays.asList(new Integer[] { DD, HH, NN, SS, MS }));

    private DateDiffUtils() {
    }

    public static String diffToString(long d1, long d2) {
        int[] t = DateDiffUtils.splitMillis(d2 - d1);
        return String.format("%d:%d:%d.%d", t[0], t[1], t[2], t[3]);
    }

    // -------------------------------------------------------------------------
    public static long dateDiff(int type, Date d1, Date d2) {
        if (d1 == null || d2 == null) {
            throw new IllegalArgumentException("dateDiff: arguments cannot be null");
        }

        return dateDiff(type, d1.getTime(), d2.getTime());
    }

    // -------------------------------------------------------------------------
    public static long millisForType(int type, int unitCount) {
        if (unitCount < 1) {
            throw new IllegalArgumentException("dateDiff: arguments cannot be less than 1");
        }

        if (!validateTimeIntervalType(type)) {
            throw new IllegalArgumentException("Invalid dateDiff type provided");
        }

        long ret = 0;

        switch (type) {
            case MS:
                ret = unitCount;
                break;
            case SS:
                ret = unitCount * MILLIS;
                break;
            case NN:
                ret = unitCount * SECONDS * MILLIS;
                break;
            case HH:
                ret = unitCount * MINUTES * SECONDS * MILLIS;
                break;
            case DD:
                ret = (unitCount * HOURS * MINUTES * SECONDS * MILLIS) & 0x00000000ffffffffL; // return unsigned
                break;
            default:
                throw new RuntimeException("DATE DIFF TYPE NOT SUPPORTED");
        }

        return ret;
    }

    // -------------------------------------------------------------------------
    public static long dateDiff(int type, long d1, long d2) {
        if (d1 < 0 || d2 < 0) {
            throw new IllegalArgumentException("dateDiff: arguments cannot be negative");
        }

        if (!validateTimeIntervalType(type)) {
            throw new IllegalArgumentException("Invalid dateDiff type provided");
        }

        long diff = Math.abs(d2 - d1),
                ret = 0;

        switch (type) {
            case MS:
                ret = diff;
                break;
            case SS:
                ret = diff / MILLIS;
                break;
            case NN:
                ret = diff / (MILLIS * SECONDS);
                break;
            case HH:
                ret = diff / (MILLIS * SECONDS * MINUTES);
                break;
            case DD:
                ret = diff / (MILLIS * SECONDS * MINUTES * HOURS);
                break;
            default:
                throw new RuntimeException("DATE DIFF TYPE NOT SUPPORTED");
        }
        // System.out.printf( "\n!!! DIFF %d TYPE %d !!!\n", ret, type );
        return ret;
    }

    // -------------------------------------------------------------------------
    public static boolean validateTimeIntervalType(int pType) {
        return _types.contains(pType);
    }

    // -------------------------------------------------------------------------
    public static int[] splitMillis(long pMillis) {
        // create unsigned long
        //
        long val = pMillis & 0x00000000ffffffffL;
        long h = val / (1000 * 3600),
                n = (val % (1000 * 3600)) / 60000,
                s = (val % 60000) / 1000,
                m = (val % (60000)) % 1000;

        int[] result = new int[] { (int) h, (int) n, (int) s, (int) m };

        return result;
    }
}
