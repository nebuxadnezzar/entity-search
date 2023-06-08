package com.entity.indexing;

import java.io.*;
import java.util.*;
import org.apache.lucene.document.*;

import com.entity.util.*;

public class HttpStreamDocumentCollector extends DocumentCollector {

    private static final String DATA_HEADER = "{\"data\":[\n";
    private static final String DATA_FOOTER = "],\"count\":\"%d\",\"status\":\"OK\"}";
    private static final int BUFF_SIZE = 20480;

    private OutputStream _os = null;
    private boolean _distinctFlag = false,
            _compressFlag = false,
            _contentOnlyFlag = false;
    private String[] _docs = new String[] { "" };
    private String _metaDataString = null;
    private ByteArrayBuilder _ba = new ByteArrayBuilder(BUFF_SIZE);
    int _idx = 0,
            _n = 0,
            _dupcnt = 0;

    public HttpStreamDocumentCollector(int pNumOfDocs, Set<String> pFilterFields, String pMetadataString) {
        super(pNumOfDocs, pFilterFields);
        _docs = new String[pNumOfDocs];
        _distinctFlag = shouldWeDedup();
        _metaDataString = pMetadataString;
        _contentOnlyFlag = SimpleUtils.isNotEmpty(pFilterFields) &&
                pFilterFields.size() == 1 &&
                pFilterFields.contains("content");
        System.out.printf("\n\n!!! CONTENT ONLY FLAG: %s\n\n", _contentOnlyFlag);
    }

    // -------------------------------------------------------------------------
    public void setSendCompressed(boolean pVal) {
        _compressFlag = pVal;
    }

    // -------------------------------------------------------------------------
    public void setOutStream(OutputStream pOs) {
        _os = pOs;

        if (_os == null) {
            throw new IllegalArgumentException("Output stream must not be null for this collector");
        }
        /* */
        // try {

        // DataStreamUtils.sendTransferHeader(pOs, HttpStatus.OK_200, "OK",
        // "application/json",
        // System.currentTimeMillis(), _compressFlag);
        // } catch (Exception e) {
        // System.out.println(e);
        // throw new RuntimeException(e);
        // }
    }

    // -------------------------------------------------------------------------
    public void collect(Document pDoc) {
        if (_os == null) {
            throw new IllegalArgumentException("Output stream must be set for this collector");
        }

        try {
            int i = _idx++;

            byte[] s = _contentOnlyFlag ? extractContentField(pDoc).getBytes("UTF-8") : docToBytes(pDoc, _filterFields);

            if (i == 0) {
                DataStreamUtils.sendDataChunk(_os, DATA_HEADER.getBytes(), _compressFlag);
            }

            if (i + 1 == _numOfDocs) {
                flushBuffer(s, i, true);
                ++_n;
                DataStreamUtils.sendDataChunk(_os,
                        String.format(DATA_FOOTER, _n).getBytes(), _compressFlag);
                DataStreamUtils.sendDataChunk(_os, null, _compressFlag);

                return;
            }

            ++_n;

            // System.out.printf( "NUM OF DOCS %d IDX %d N %d DUPS %d\n", _numOfDocs, i, _n,
            // _dupcnt );
            flushBuffer(s, i, false);
        } catch (Exception e) {
            System.err.println("EXCEPTION IN " + this.getClass().getName() + " " + e);
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    // -------------------------------------------------------------------------
    private void flushBuffer(byte[] b, int docCnt, boolean force) throws IOException {
        if (docCnt > 0) {
            _ba.append(comma);
        }
        _ba.append(b);

        if (_ba.size() >= BUFF_SIZE || force) {
            DataStreamUtils.sendDataChunk(_os, _ba.toByteArray(), _compressFlag);
            _ba.reset();
        }
    }

    // -------------------------------------------------------------------------
    private String extractContentField(Document pDoc) {
        Document doc = pDoc;
        String[] vals = doc.getValues("content");
        StringBuilder sb = new StringBuilder();

        for (int i = 0, k = vals.length; i < k; i++) {
            final String s = JsonEscapeUtils.unescape(vals[i]);

            sb.append(s);
            if (i < k - 1) {
                sb.append(',');
            }
        }
        sb.append('\n');
        return sb.toString();
    }

    // -------------------------------------------------------------------------
    private boolean isDocDup(byte[] bytes) {
        boolean ret = false;
        if (_distinctFlag) {
            long hash = getHash(bytes);
            if (_hashes.contains(hash)) {
                ++_dupcnt;
                ret = true;
            }
            _hashes.add(hash);
        }
        return ret;
    }

    // -------------------------------------------------------------------------
    public String[] getData() {
        // nothing was found
        //
        if (_numOfDocs == 0) {
            try {
                DataStreamUtils.sendDataChunk(_os, (DATA_HEADER + String.format(DATA_FOOTER, 0)).getBytes(),
                        _compressFlag);
                DataStreamUtils.sendDataChunk(_os, null, _compressFlag);
            } catch (IOException e) {
                e.printStackTrace();
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
        // this stream should be closed by HTTP handling Worker thread
        if (_os != null) {
            try {
                _os.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
