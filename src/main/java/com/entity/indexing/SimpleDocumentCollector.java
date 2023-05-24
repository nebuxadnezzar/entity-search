package com.entity.indexing;

import java.io.*;
import java.util.*;
import org.apache.lucene.document.*;

public class SimpleDocumentCollector extends DocumentCollector {
    OutputStream _os = null;
    boolean _distinctFlag = false;
    String[] _docs = new String[] { "" };
    int _idx = 0;
    byte[] _recSep = new byte[] { 0x2C };

    public SimpleDocumentCollector(int pNumOfDocs, Set<String> pFilterFields) {
        super(pNumOfDocs, pFilterFields);
        _docs = new String[pNumOfDocs];
        _distinctFlag = shouldWeDedup();
        // System.out.println( "DISTINCT FLAG " + _distinctFlag );
        // System.out.println( "FILTER FIELDS " + pFilterFields );
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
        String s = docToString(pDoc, _filterFields);

        if (_distinctFlag) {
            long hash = getHash(s);
            if (_hashes.contains(hash)) {
                return;
            }
            _hashes.add(hash);
        }
        /**/
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
    public String[] getData() {
        if (_os == null) {
            if (_docs.length > _idx) {
                String[] t = new String[_idx];
                System.arraycopy(_docs, 0, t, 0, _idx);
                // Arrays.sort( t );
                return t;
            }
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
