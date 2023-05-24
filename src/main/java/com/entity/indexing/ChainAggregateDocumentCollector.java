package com.entity.indexing;

import java.io.*;
import java.util.*;
import org.apache.lucene.document.*;

public class ChainAggregateDocumentCollector extends DocumentCollector {
    private List<DocumentCollector> _resultSets = new ArrayList<DocumentCollector>();

    public ChainAggregateDocumentCollector(int pNumOfDocs, Map<String, String> pFieldMap) {
        super(pNumOfDocs, null);

        if (pFieldMap == null || pFieldMap.size() < 1) {
            throw new RuntimeException("empty FieldMap");
        }

        for (Map.Entry<String, String> e : pFieldMap.entrySet()) {
            Map<String, String> m = new HashMap<String, String>();
            m.put("aggFields", e.getValue());
            _resultSets.add(new AggregateDocumentCollector(pNumOfDocs, m));
        }
    }

    // -------------------------------------------------------------------------
    public void setOutStream(OutputStream pOs) {
    }

    // -------------------------------------------------------------------------
    public void collect(Document pDoc) {
        for (DocumentCollector d : _resultSets) {
            d.collect(pDoc);
        }
    }

    // -------------------------------------------------------------------------
    @SuppressWarnings("unchecked")
    public String[] getData() {

        int size = 0;
        for (DocumentCollector d : _resultSets) {
            size += ((Map<String, Object>) d.getResultSet()).size();
        }

        String[] t = new String[size];
        int offset = 0;

        for (DocumentCollector d : _resultSets) {
            String[] a = d.getData();
            System.arraycopy(a, 0, t, offset, a.length);
            offset += a.length;
        }

        if (offset < size) {
            t = Arrays.copyOf(t, offset);
        }

        return t;
    }

    // -------------------------------------------------------------------------
    public Object getResultSet() {
        if (_resultSets.size() == 1) {
            return _resultSets.get(0);
        }

        if (_resultSets.size() > 1) {
            return _resultSets;
        } else {
            return new HashMap<String, Object>();
        }
    }

    // ------------------------------------------------------------------------
    public void closeOutStream() {/*
                                   * if( _os != null )
                                   * {
                                   * try{ _os.close() }catch( Exception e ){System.out.println( e ); }
                                   * }
                                   */
    }
}
