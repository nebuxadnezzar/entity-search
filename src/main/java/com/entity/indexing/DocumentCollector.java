package com.entity.indexing;

import java.io.*;
import java.util.*;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import com.entity.util.*;

public abstract class DocumentCollector {
    private static final int MAX_FIELDS_TO_DEDUP = 5;
    protected static final byte[] comma = new byte[] { 0x2C };

    private int HASH_SEED = (int) System.currentTimeMillis();

    protected int _numOfDocs = 0;
    protected Set<String> _filterFields = null;
    protected Set<Long> _hashes = new HashSet<Long>();
    protected String _recSeparator = "\n";

    public DocumentCollector(int pNumberOfDocs, Set<String> pFilterFields) {
        _numOfDocs = pNumberOfDocs;
        _filterFields = pFilterFields;
    }

    // -------------------------------------------------------------------------
    public void setRecSeparator(String pSep) {
        if (pSep != null) {
            _recSeparator = pSep;
        }
    }

    // -------------------------------------------------------------------------
    public int getNumberOfDocs() {
        return _numOfDocs;
    }

    // -------------------------------------------------------------------------
    public void setNumberOfDocs(int pNumberOfDocs) {
        _numOfDocs = pNumberOfDocs > 0 ? pNumberOfDocs : 0;
    }

    // -------------------------------------------------------------------------
    protected long getHash(byte[] bytes) {
        if (bytes == null || bytes.length < 1) {
            return 0;
        }

        // return StringUtils.murmurhash3_x86_32( bytes, 0, bytes.length, HASH_SEED );
        return StringUtils.hash64(bytes, bytes.length, HASH_SEED);
    }

    // -------------------------------------------------------------------------
    protected long getHash(String str) {
        if (str == null) {
            return 0;
        }

        // murmur hash is more collision prone when string length is divisible by 8
        //
        String t = !(str.length() % 8 == 0) ? str : str + "z";

        // return StringUtils.murmurhash3_x86_32( t.getBytes(), 0, str.length(),
        // HASH_SEED );
        return StringUtils.hash64(t.getBytes(), str.length(), HASH_SEED);
    }

    // -------------------------------------------------------------------------
    // -- if we choose small amount of filter fields we may get lot's of dups
    // -------------------------------------------------------------------------
    protected boolean shouldWeDedup() {
        return (_filterFields != null && _filterFields.size() <= MAX_FIELDS_TO_DEDUP);
    }

    // -------------------------------------------------------------------------
    public static byte[] docToBytes(Document doc, Set<String> filterFields) {
        List<IndexableField> fields = doc.getFields();
        byte[] sep = "\",\"".getBytes();
        ByteArrayBuilder sb = new ByteArrayBuilder("{");
        Set<String> names = getFieldNames(doc);

        if (!SimpleUtils.isEmpty(filterFields)) {
            names.retainAll(filterFields);
        }

        int i = 1, k = names.size();

        for (String name : names) {
            sb.append("\"").append(name).append("\":[\"");
            try {
                sb.append(StringUtils.joinB(doc.getValues(name), sep));
            } catch (Exception e) {
                System.err.println("docToBytes(): " + e);
            }
            sb.append("\"]");
            if (i++ < k) {
                sb.append(comma);
            }
        }
        sb.append("}");

        return sb.toByteArray();
    }

    // -------------------------------------------------------------------------
    public static String docToString(Document doc, Set<String> filterFields) {
        // List<IndexableField> fields = doc.getFields();

        StringBuilder sb = new StringBuilder("{");
        Set<String> names = getFieldNames(doc);

        if (!SimpleUtils.isEmpty(filterFields)) {
            names.retainAll(filterFields);
        }

        // System.out.println( "FIELD NAMES " + names );

        int i = 1, k = names.size();

        for (String name : names) {
            sb.append("\"" + name + "\":[\"" + StringUtils.join(doc.getValues(name), "\",\"") + "\"]");

            if (i++ < k) {
                sb.append(",");
            }
        }
        sb.append("}");

        return sb.toString();
    }

    // -------------------------------------------------------------------------
    public static String docToTokenString(Document doc, Set<String> filterFields) {

        StringBuilder sb = new StringBuilder();
        Set<String> names = getFieldNames(doc);

        if (!SimpleUtils.isEmpty(filterFields)) {
            names.retainAll(filterFields);
        }

        // if filter fields are linked hash set the field ordering will be enforced
        //
        if (names.size() > 0 && filterFields != null) {
            names = filterFields;
        }

        int i = 1, k = names.size();

        for (String name : names) {
            sb.append(StringUtils.join(doc.getValues(name), ","));

            if (i++ < k) {
                sb.append("|");
            }
        }

        return sb.toString();
    }

    // -------------------------------------------------------------------------
    protected static String fieldToString(IndexableField f) {
        if (f == null) {
            return "";
        }
        String val = null;

        if (f.stringValue() != null) {
            val = f.stringValue();
        } else

        if (f.numericValue() != null) {
            val = f.numericValue().toString();
        } else

        if (f.readerValue() != null) {
            val = f.readerValue().toString();
        }

        return val;
    }

    // -------------------------------------------------------------------------
    public static Set<String> getFieldNames(Document doc) {
        Set<String> names = new LinkedHashSet<String>();

        if (doc == null) {
            return names;
        }

        List<IndexableField> fields = doc.getFields();

        for (IndexableField f : fields) {
            names.add(f.name());
        }

        return names;
    }
    // -------------------------------------------------------------------------
    // - three methods below taken straight from JVM code
    // -------------------------------------------------------------------------

    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    protected String[] ensureCapacity(int minCapacity, String[] buf) {
        String[] b = buf;
        // overflow-conscious code
        if (minCapacity - buf.length > 0) {
            b = grow(minCapacity, buf);
        }
        return b;
    }

    private String[] grow(int minCapacity, String[] buf) {
        // overflow-conscious code
        int oldCapacity = buf.length;
        int newCapacity = oldCapacity << 1;
        if (newCapacity - minCapacity < 0)
            newCapacity = minCapacity;
        if (newCapacity - MAX_ARRAY_SIZE > 0)
            newCapacity = hugeCapacity(minCapacity);
        buf = Arrays.copyOf(buf, newCapacity);

        return buf;
    }

    private static int hugeCapacity(int minCapacity) {
        if (minCapacity < 0) // overflow
        {
            throw new OutOfMemoryError();
        }
        return (minCapacity > MAX_ARRAY_SIZE) ? Integer.MAX_VALUE : MAX_ARRAY_SIZE;
    }

    /**/
    // -------------------------------------------------------------------------
    public abstract void collect(Document pDoc);

    public abstract String[] getData();

    public abstract Object getResultSet();

    public abstract void setOutStream(OutputStream pOs);

    public abstract void closeOutStream();
}
