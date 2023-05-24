package com.entity.util;

import java.io.*;
import java.util.*;

public class DataStreamUtils {

    private DataStreamUtils() {
    }

    // -------------------------------------------------------------------------
    public static void sendTransferHeader(OutputStream out, int pCode,
            String description,
            String contentType,
            long lastModified,
            boolean pSendCompressed)
            throws IOException {
        String header = String.format(HTTP_TRANSFER_TEMPLATE, pCode,
                description,
                new Date().toString(),
                contentType,
                new Date(lastModified).toString(),
                (pSendCompressed ? "gzip, chunked" : "chunked"));
        // System.out.printf( "HEADER START:\n%sHEADER END\n", header );
        out.write(header.getBytes());
        out.write(HEADER_FOOTER);
        out.flush();
    }

    // -------------------------------------------------------------------------
    public static void sendDataChunk(OutputStream out, byte[] data, boolean pSendCompressed)
            throws IOException {
        int len = SimpleUtils.isNotEmpty(data) ? data.length : 0;

        if (len > 0 && pSendCompressed) {
            ByteArrayOutputStream bo = new ByteArrayOutputStream(512);
            java.util.zip.GZIPOutputStream gzip = new java.util.zip.GZIPOutputStream(bo);
            gzip.write(data);
            gzip.close();
            data = bo.toByteArray();
            len = data.length;
        }
        // out.write(String.format("%s\r\n", Integer.toHexString(len)).getBytes());
        if (len > 0) {
            out.write(data);
        }
        // out.write(CHUNK_FOOTER);
        out.flush();
    }

    // private static final byte[] CHUNK_FOOTER = new byte[] { 0xD, 0xA };
    private static final byte[] HEADER_FOOTER = new byte[] { 0xA };
    private static final String HTTP_TRANSFER_TEMPLATE = "HTTP/1.1 %d %s\nDate: %s\nContent-type: %s\n" +
            "Last-modified: %s\nTransfer-Encoding: %s\n";
}
