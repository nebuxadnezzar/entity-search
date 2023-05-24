package com.entity.util;

/*
 * Copyright 2001-2004 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;

/**
 *
 * 
 * 
 * This class provides static utility methods for buffered
 * copying between sources (InputStream, Reader, String and
 * byte[]) and destinations (OutputStream, Writer,
 * String and byte[]).
 *
 *
 *
 * 
 * 
 * Unless otherwise noted, these copy methods do not flush or close the
 * streams. Often doing so would require making non-portable assumptions about
 * the streams' origin
 * and further use. This means that both streams' close() methods must be called
 * after
 * copying. if one omits this step, then the stream resources (sockets, file
 * descriptors) are
 * released when the associated Stream is garbage-collected. It is not a good
 * idea to rely on this
 * mechanism. For a good overview of the distinction between "memory management"
 * and "resource
 * management", see this
 * UnixReview article.
 *
 *
 * 
 * 
 * For byte-to-char methods, a copy variant allows the encoding
 * to be selected (otherwise the platform default is used). We would like to
 * encourage you to always specify the encoding because relying on the platform
 * default can lead to unexpected results.
 *
 *
 * 
 * 
 * We don't provide special variants for the copy methods that
 * let you specify the buffer size because in modern VMs the impact on speed
 * seems to be minimal. We're using a default buffer size of 4 KB.
 *
 *
 * 
 * 
 * The copy methods use an internal buffer when copying. It is therefore
 * advisable
 * not to deliberately wrap the stream arguments to the copy methods in
 * Buffered* streams. For example, don't do the
 * following:
 *
 * 
 * copy( new BufferedInputStream( in ), new BufferedOutputStream( out ) );
 *
 *
 * 
 * The rationale is as follows:
 *
 *
 * 
 * 
 * Imagine that an InputStream's read() is a very expensive operation, which
 * would usually suggest
 * wrapping in a BufferedInputStream. The BufferedInputStream works by issuing
 * infrequent
 * {@link java.io.InputStream#read(byte[] b, int off, int len)} requests on the
 * underlying InputStream, to
 * fill an internal buffer, from which further read requests can inexpensively
 * get
 * their data (until the buffer runs out).
 *
 * 
 * 
 * However, the copy methods do the same thing, keeping an internal buffer,
 * populated by {@link InputStream#read(byte[] b, int off, int len)} requests.
 * Having two buffers
 * (or three if the destination stream is also buffered) is pointless, and the
 * unnecessary buffer
 * management hurts performance slightly (about 3%, according to some simple
 * experiments).
 *
 *
 * 
 * 
 * Behold, intrepid explorers; a map of this class:
 *
 * 
 * Method Input Output Dependency
 * ------ ----- ------ -------
 * 1 copy InputStream OutputStream (primitive)
 * 2 copy Reader Writer (primitive)
 *
 * 3 copy InputStream Writer 2
 *
 * 4 copy Reader OutputStream 2
 *
 * 5 copy String OutputStream 2
 * 6 copy String Writer (trivial)
 *
 * 7 copy byte[] Writer 3
 * 8 copy byte[] OutputStream (trivial)
 *
 *
 *
 * 
 * Note that only the first two methods shuffle bytes; the rest use these
 * two, or (if possible) copy using native Java copy methods. As there are
 * method variants to specify the encoding, each row may
 * correspond to up to 2 methods.
 *
 *
 * 
 * 
 * Origin of code: Apache Avalon (Excalibur)
 *
 * 
 * @author Peter Donald
 * @author Jeff Turner
 * @author Matthew Hawthorne
 * @version $Id: CopyUtils.java,v 1.6 2004/04/24 23:49:25 bayard Exp $
 */
public class CopyUtils {

    /**
     * The name says it all.
     */
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 4;

    /**
     * Instances should NOT be constructed in standard programming.
     */
    public CopyUtils() {
    }

    // ----------------------------------------------------------------
    // byte[] -> OutputStream
    // ----------------------------------------------------------------

    /**
     * Copy bytes from a byte[] to an OutputStream.
     * 
     * @param input  the byte array to read from
     * @param output the OutputStream to write to
     * @throws IOException In case of an I/O problem
     */
    public static void copy(byte[] input, OutputStream output)
            throws IOException {
        output.write(input);
    }

    // ----------------------------------------------------------------
    // byte[] -> Writer
    // ----------------------------------------------------------------

    /**
     * Copy and convert bytes from a byte[] to chars on a
     * Writer.
     * The platform's default encoding is used for the byte-to-char conversion.
     * 
     * @param input  the byte array to read from
     * @param output the Writer to write to
     * @throws IOException In case of an I/O problem
     */
    public static void copy(byte[] input, Writer output)
            throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(input);
        copy(in, output);
    }

    /**
     * Copy and convert bytes from a byte[] to chars on a
     * Writer, using the specified encoding.
     * 
     * @param input    the byte array to read from
     * @param output   the Writer to write to
     * @param encoding The name of a supported character encoding. See the
     *                 IANA
     *                 Charset Registry for a list of valid encoding types.
     * @throws IOException In case of an I/O problem
     */
    public static void copy(
            byte[] input,
            Writer output,
            String encoding)
            throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(input);
        copy(in, output, encoding);
    }

    // ----------------------------------------------------------------
    // Core copy methods
    // ----------------------------------------------------------------

    /**
     * Copy bytes from an InputStream to an OutputStream.
     * 
     * @param input  the InputStream to read from
     * @param output the OutputStream to write to
     * @return the number of bytes copied
     * @throws IOException In case of an I/O problem
     */
    public static int copy(
            InputStream input,
            OutputStream output)
            throws IOException {
        byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
        int count = 0;
        int n = 0;
        while (-1 != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
            count += n;
        }
        return count;
    }

    // ----------------------------------------------------------------
    // Reader -> Writer
    // ----------------------------------------------------------------

    /**
     * Copy chars from a Reader to a Writer.
     * 
     * @param input  the Reader to read from
     * @param output the Writer to write to
     * @return the number of characters copied
     * @throws IOException In case of an I/O problem
     */
    public static int copy(
            Reader input,
            Writer output)
            throws IOException {
        char[] buffer = new char[DEFAULT_BUFFER_SIZE];
        int count = 0;
        int n = 0;
        while (-1 != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
            count += n;
        }
        return count;
    }

    // ----------------------------------------------------------------
    // InputStream -> Writer
    // ----------------------------------------------------------------

    /**
     * Copy and convert bytes from an InputStream to chars on a
     * Writer.
     * The platform's default encoding is used for the byte-to-char conversion.
     * 
     * @param input  the InputStream to read from
     * @param output the Writer to write to
     * @throws IOException In case of an I/O problem
     */
    public static void copy(
            InputStream input,
            Writer output)
            throws IOException {
        InputStreamReader in = new InputStreamReader(input);
        copy(in, output);
    }

    /**
     * Copy and convert bytes from an InputStream to chars on a
     * Writer, using the specified encoding.
     * 
     * @param input    the InputStream to read from
     * @param output   the Writer to write to
     * @param encoding The name of a supported character encoding. See the
     *                 IANA
     *                 Charset Registry for a list of valid encoding types.
     * @throws IOException In case of an I/O problem
     */
    public static void copy(
            InputStream input,
            Writer output,
            String encoding)
            throws IOException {
        InputStreamReader in = new InputStreamReader(input, encoding);
        copy(in, output);
    }

    // ----------------------------------------------------------------
    // Reader -> OutputStream
    // ----------------------------------------------------------------

    /**
     * Serialize chars from a Reader to bytes on an
     * OutputStream, and flush the OutputStream.
     * 
     * @param input  the Reader to read from
     * @param output the OutputStream to write to
     * @throws IOException In case of an I/O problem
     */
    public static void copy(
            Reader input,
            OutputStream output)
            throws IOException {
        OutputStreamWriter out = new OutputStreamWriter(output);
        copy(input, out);
        // XXX Unless anyone is planning on rewriting OutputStreamWriter, we have to
        // flush here.
        out.flush();
    }

    // ----------------------------------------------------------------
    // String -> OutputStream
    // ----------------------------------------------------------------

    /**
     * Serialize chars from a String to bytes on an OutputStream, and
     * flush the OutputStream.
     * 
     * @param input  the String to read from
     * @param output the OutputStream to write to
     * @throws IOException In case of an I/O problem
     */
    public static void copy(
            String input,
            OutputStream output)
            throws IOException {
        StringReader in = new StringReader(input);
        OutputStreamWriter out = new OutputStreamWriter(output);
        copy(in, out);
        // XXX Unless anyone is planning on rewriting OutputStreamWriter, we have to
        // flush here.
        out.flush();
    }

    // ----------------------------------------------------------------
    // String -> Writer
    // ----------------------------------------------------------------

    /**
     * Copy chars from a String to a Writer.
     * 
     * @param input  the String to read from
     * @param output the Writer to write to
     * @throws IOException In case of an I/O problem
     */
    public static void copy(String input, Writer output)
            throws IOException {
        output.write(input);
    }

} // CopyUtils