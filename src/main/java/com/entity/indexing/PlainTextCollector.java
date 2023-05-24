package com.entity.indexing;

import java.io.*;
import java.util.*;
import org.apache.lucene.document.*;

public class PlainTextCollector extends DocumentCollector {
    OutputStream _os = null;
    String[] _docs = new String[] { "" };
    int _idx = 0;
    boolean _distinctFlag = false;
    byte[] _recSep = new byte[] { 0xA };
    Map<String, Integer> _m = new HashMap<String, Integer>();

    public PlainTextCollector(int pNumOfDocs, Set<String> pFilterFields) {
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
        String s = docToTokenString(pDoc, _filterFields);

        if (_distinctFlag) {// _m.put( s, getHash( s ));
            long hash = getHash(s);
            if (_hashes.contains(hash)) {
                return;
            }
            _hashes.add(hash);
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
    public String[] getData() {// for( String s : _docs ){System.out.println( "###> " + s ); }
                               // System.out.println( "!!! HASH MAP " + _m );

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
