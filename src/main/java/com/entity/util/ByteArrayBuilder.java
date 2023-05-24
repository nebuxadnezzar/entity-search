package com.entity.util;

import java.io.*;
import java.util.Arrays;

public class ByteArrayBuilder {
    private ByteArrayOutput buff = null;

    public ByteArrayBuilder() {
        buff = new ByteArrayOutput();
    }

    // -------------------------------------------------------------------------
    public ByteArrayBuilder(int size) {
        buff = new ByteArrayOutput(size);
    }

    // -------------------------------------------------------------------------
    public ByteArrayBuilder(byte[] bytes) {
        buff = new ByteArrayOutput();
        append(bytes);
    }

    // -------------------------------------------------------------------------
    public ByteArrayBuilder(Integer size) {
        buff = new ByteArrayOutput(size.intValue());
    }

    // -------------------------------------------------------------------------
    public ByteArrayBuilder(String s) {
        buff = new ByteArrayOutput();
        append(s);
    }

    // -------------------------------------------------------------------------
    public ByteArrayBuilder append(ByteArrayBuilder b) {
        byte[] bb = b.toByteArray();
        int len = bb.length;
        buff.write(bb, 0, len);
        return this;
    }

    // -------------------------------------------------------------------------
    public ByteArrayBuilder append(String s) {
        try {
            return append(s.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException ue) {
            return append(s.getBytes());
        }
    }

    // -------------------------------------------------------------------------
    public ByteArrayBuilder append(byte[] b) {
        int len = b.length;
        buff.write(b, 0, len);
        return this;
    }

    // -------------------------------------------------------------------------
    public ByteArrayBuilder append(int b) {
        buff.write(b);
        return this;
    }

    // -------------------------------------------------------------------------
    public ByteArrayBuilder append(Integer b) {
        return append(b.intValue());
    }

    // -------------------------------------------------------------------------
    public byte chop() {
        return buff.chop();
    }

    // -------------------------------------------------------------------------
    public void reset() {
        buff.reset();
    }

    // -------------------------------------------------------------------------
    public int size() {
        return buff.size();
    }

    // -------------------------------------------------------------------------
    public byte[] toByteArray() {
        return buff.toByteArray();
    }

    // -------------------------------------------------------------------------
    public String toString() {
        return buff.toString();
    }
}

// desynchronized version of ByteArrayOutputStream
// done for speed improvement
// not thread safe
//
class ByteArrayOutput {
    private byte buf[];
    private int count;

    public ByteArrayOutput() {
        this(32);
    }

    public ByteArrayOutput(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("Negative initial size: "
                    + size);
        }
        buf = new byte[size];
    }

    private void ensureCapacity(int minCapacity) {
        // overflow-conscious code
        if (minCapacity - buf.length > 0)
            grow(minCapacity);
    }

    /**
     * The maximum size of array to allocate.
     * Some VMs reserve some header words in an array.
     * Attempts to allocate larger arrays may result in
     * OutOfMemoryError: Requested array size exceeds VM limit
     */
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    /**
     * Increases the capacity to ensure that it can hold at least the
     * number of elements specified by the minimum capacity argument.
     *
     * @param minCapacity the desired minimum capacity
     */
    private void grow(int minCapacity) {
        // overflow-conscious code
        int oldCapacity = buf.length;
        int newCapacity = oldCapacity << 1;
        if (newCapacity - minCapacity < 0)
            newCapacity = minCapacity;
        if (newCapacity - MAX_ARRAY_SIZE > 0)
            newCapacity = hugeCapacity(minCapacity);
        buf = Arrays.copyOf(buf, newCapacity);
    }

    private static int hugeCapacity(int minCapacity) {
        if (minCapacity < 0) // overflow
            throw new OutOfMemoryError();
        return (minCapacity > MAX_ARRAY_SIZE) ? Integer.MAX_VALUE : MAX_ARRAY_SIZE;
    }

    public void write(int b) {
        ensureCapacity(count + 1);
        buf[count] = (byte) b;
        count += 1;
    }

    public void write(byte b[], int off, int len) {
        if ((off < 0) || (off > b.length) || (len < 0) ||
                ((off + len) - b.length > 0)) {
            throw new IndexOutOfBoundsException();
        }
        ensureCapacity(count + len);
        System.arraycopy(b, off, buf, count, len);
        count += len;
    }

    public void reset() {
        count = 0;
    }

    public byte[] toByteArray() {
        return Arrays.copyOf(buf, count);
    }

    public byte chop() {
        if (count < 1) {
            throw new IndexOutOfBoundsException("buffer is empty");
        }
        --count;
        return buf[count];
    }

    public int size() {
        return count;
    }

    public String toString() {
        return new String(buf, 0, count);
    }

    public String toString(String charsetName)
            throws UnsupportedEncodingException {
        return new String(buf, 0, count, charsetName);
    }
}
