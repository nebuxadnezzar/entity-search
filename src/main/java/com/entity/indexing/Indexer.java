package com.entity.indexing;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.math.*;
import org.apache.lucene.document.*;

import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.analysis.Analyzer;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.util.*;
import org.apache.lucene.store.*;

import com.entity.util.StringUtils;

public class Indexer {

    public enum DATATYPE {
        STRING, TEXT, FLOAT, INT, BINARY
    };

    private static final Set<String> TRUE_TYPES = new HashSet<String>(Arrays.asList("yes", "y", "true", "t")),
            DATA_TYPES = new HashSet<String>(Arrays.asList("string", "text", "float", "int", "symbol")),
            FIELD_OPTS = new HashSet<String>(Arrays.asList("index", "store", "name", "type", "raw", "sort"));

    IndexWriter indexWriter = null;

    public Indexer(String index_dir) throws IOException {
        this(index_dir, null, true);
    }

    public Indexer(String index_dir, Analyzer analyzer_, boolean create)
            throws IOException {
        indexWriter = createIndexWriter(index_dir, analyzer_, create, true);
    }

    // -------------------------------------------------------------------------
    public static IndexWriter createIndexWriter(String index_dir, Analyzer analyzer_, boolean create, boolean doCommits)
            throws IOException {
        Analyzer analyzer = (analyzer_ != null ? analyzer_ : new StandardAnalyzer());
        // ( ( StandardAnalyzer )analyzer ).setMaxTokenLength( 80000 );
        // System.out.println( "!!! ANALYZER !!! " + analyzer );
        //
        Directory indexDir = FSDirectory.open(FileSystems.getDefault().getPath(index_dir));
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setRAMBufferSizeMB(256.0D);
        LogMergePolicy mergePolicy = new LogByteSizeMergePolicy();
        mergePolicy.setMergeFactor(12);
        ((LogByteSizeMergePolicy) mergePolicy).setMaxMergeMB(1000D);
        ((LogByteSizeMergePolicy) mergePolicy).setMinMergeMB(10D);
        config.setMergePolicy(mergePolicy);

        if (create) {
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        } else {
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        }

        config.setCommitOnClose(doCommits);

        return new IndexWriter(indexDir, config);
    }

    // -------------------------------------------------------------------------
    public void commit() throws IOException {
        indexWriter.commit();
    }

    // -------------------------------------------------------------------------
    public IndexWriter getIndexWriter() {
        return indexWriter;
    }

    // -------------------------------------------------------------------------
    public static boolean validateTrueType(String val) {
        return (StringUtils.isNotBlank(val) && TRUE_TYPES.contains(val.toLowerCase()));
    }

    // -------------------------------------------------------------------------
    public static boolean validateDataType(String val) {
        return (StringUtils.isNotBlank(val) && DATA_TYPES.contains(val.toLowerCase()));
    }

    // -------------------------------------------------------------------------
    public static boolean validateFieldOpts(String val) {
        return (StringUtils.isNotBlank(val) && FIELD_OPTS.contains(val.toLowerCase()));
    }

    // -------------------------------------------------------------------------
    public void indexDoc(Document doc) throws IOException { // System.out.println( "!!! DOCUMENT !!! " + doc );
                                                            // System.out.println( "!!! WRITER !!! " + indexWriter );
        try {
            indexWriter.addDocument(doc);
        } catch (Exception e) {
            System.err.println(doc);
            throw new RuntimeException(e);
        }
        // getIndexWriter().addDocument( doc );
    }

    // -------------------------------------------------------------------------
    public void indexDoc(Document doc, String fieldName, String fieldValue) throws IOException {
        try { // System.out.printf( "\nUPDATING %s %s %s\n", fieldName, fieldValue,
              // doc.toString() );
              // System.out.printf( "TERM %s\n", new Term( fieldName, fieldValue ) );
            indexWriter.updateDocument(new Term(fieldName, fieldValue), doc);
        } catch (Exception e) {
            System.err.println(doc);
            throw new RuntimeException(e);
        }
    }

    // -------------------------------------------------------------------------
    public void closeIndexWriter() throws IOException {
        if (indexWriter != null) {
            indexWriter.commit();
            /* indexWriter.forceMergeDeletes(); */ indexWriter.close();
        }
    }

    // -------------------------------------------------------------------------
    public void createDummyDoc() throws IOException {
        Map<String, String> m = new HashMap<String, String>();
        Map<String, Map<String, String>> mm = new HashMap<String, Map<String, String>>();
        java.security.SecureRandom random = new java.security.SecureRandom();
        String key = "dummy", // new BigInteger(130, random).toString(32),
                val = new BigInteger(130, random).toString(32);

        m.put("index", "true");
        m.put("store", "true");
        m.put("type", "string");

        mm.put(key, m);
        Document d = addToDoc(null, mm, key, val);
        indexDoc(d);
    }

    // -------------------------------------------------------------------------
    /**
     * @params - lucene Document, could be null,
     *         Map in format [ field_name : [ index:true/false/yes/no,
     *         store:true/false/yes/no, type:string/text/float/int] ],
     *         String field_name,
     *         String value to add to doc
     */
    // -------------------------------------------------------------------------
    public static Document addToDoc(Document doc,
            Map<String, Map<String, String>> schema,
            String field_name,
            String val) {
        Document d = doc;

        if (d == null) {
            d = new Document();
        }

        Map<String, String> m = schema.get(field_name);

        if (!(m != null && validateTrueType(m.get("index")) && StringUtils.isNotBlank(val))) {
            return d;
        }

        String field_type = m.get("type");
        boolean storeyn = validateTrueType(m.get("store"));
        boolean sortyn = validateTrueType(m.get("sort"));
        Field field = null/* d.getField( field_name ) */;

        // System.out.printf( "\n\t%s %s %s\n", field_name, val, String.valueOf( storeyn
        // ) );

        if (field_type.equals("symbol")) {
            FieldType ft = new FieldType();
            ft.setTokenized(false);
            ft.setStored(storeyn);
            ft.setIndexOptions(IndexOptions.NONE);
            field = new StoredField(field_name, val, ft);
            // NONE, DOCS, DOCS_AND_FREQS, DOCS_AND_FREQS_AND_POSITIONS,
            // DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS
        } else

        if (field_type.equals("string")) {
            field = new StringField(field_name, val, storeyn ? Field.Store.YES : Field.Store.NO);
        } else

        if (field_type.equals("text")) {
            field = new TextField(field_name, val, storeyn ? Field.Store.YES : Field.Store.NO);
        } else

        if (field_type.equals("int")) {
            long v = StringUtils.isNumber(val) ? Long.valueOf(val).longValue() : 0L;
            field = new LongPoint(field_name, v);
            if (storeyn) {
                d.add(new StoredField(field_name, v));
            }
        } else

        if (field_type.equals("float")) {
            double v = StringUtils.isNumber(val) ? Double.valueOf(val).doubleValue() : 0.0D;
            field = new DoublePoint(field_name, v);
            if (storeyn) {
                d.add(new StoredField(field_name, v));
            }
        } /*
           * else
           *
           * if( field_type.equals( "sort" ) )
           * {
           * field = new SortedDocValuesField( field_name, new BytesRef( val ));
           * d.add( new TextField( field_name, val, storeyn ? Field.Store.YES :
           * Field.Store.NO ) );
           * }
           */
        else {
            throw new RuntimeException("Unsupported field type: " + field_type);
        }

        d.add(field);

        if (sortyn) {
            d.add(new SortedDocValuesField(field_name, new BytesRef(val)));
        }
        return d;
    }
    // -------------------------------------------------------------------------
}
