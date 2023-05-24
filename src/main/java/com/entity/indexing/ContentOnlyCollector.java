package com.entity.indexing;

import java.io.*;
import java.util.*;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import com.entity.util.*;

// extracts data from "content" field of the document and unescapes it from JSON
//
public class ContentOnlyCollector extends DocumentCollector {
    OutputStream _os = null;
    String[] _docs = new String[] { "" };
    int _idx = 0;
    boolean _distinctFlag = false;
    byte[] _recSep = new byte[] { 0x2C };

    public ContentOnlyCollector(int pNumOfDocs, Set<String> pFilterFields) {
        super(pNumOfDocs, pFilterFields);
        _docs = new String[pNumOfDocs];
        _distinctFlag = shouldWeDedup();
    }

    // -------------------------------------------------------------------------
    public void setRecSeparator(String pSep) {
        if (pSep != null) {
            _recSep = pSep.getBytes();
        }
    }

    // -------------------------------------------------------------------------
    public void setOutStream(OutputStream pOs) {
        _os = pOs;
    }

    // -------------------------------------------------------------------------
    public void collect(Document pDoc) {
        String s = extractContentField(pDoc);

        if (StringUtils.isBlank(s)) {
            s = docToString(pDoc, null);
        }

        if (_os != null) {
            try {
                _os.write(s.getBytes("UTF-8"));
                if (_idx++ + 1 < _numOfDocs) {
                    _os.write(_recSep);
                }
            } catch (Exception e) {
                System.err.println(e);
            }
        } else {
            String[] a = ensureCapacity(_idx + 1, _docs);
            if (a != _docs) {
                _docs = a;
            }
            _docs[_idx++] = s;
        }
    }

    // -------------------------------------------------------------------------
    private String extractContentField(Document pDoc) {
        Document doc = pDoc;

        // List<IndexableField> fields = doc.getFields();

        String[] vals = doc.getValues("content");
        StringBuilder sb = new StringBuilder();

        for (int i = 0, k = vals.length; i < k; i++) {
            final String s = JsonEscapeUtils.unescape(vals[i]);

            sb.append(s);
            if (i < k - 1) {
                sb.append(',');
            }
        }
        return sb.toString();
    }

    // -------------------------------------------------------------------------
    public String[] getData() {
        if (_os == null) {
            if (_docs.length > _idx) {
                String[] t = new String[_idx];
                System.arraycopy(_docs, 0, t, 0, _idx);
                Arrays.sort(t);
                return t;
            }
            Arrays.sort(_docs);
        }
        return _docs;
    }

    // -------------------------------------------------------------------------
    public Object getResultSet() {
        return _docs;
    }

    // ------------------------------------------------------------------------
    public void closeOutStream() {
        if (_os != null) {
            try {
                _os.close();
            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }
}
