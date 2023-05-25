package com.entity.indexing;

import java.io.*;
import java.nio.file.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.*;
import java.util.AbstractMap.*;
import org.apache.lucene.document.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.*;
import org.apache.lucene.store.*;
import org.apache.lucene.search.*;

import com.entity.util.*;

public class Searcher {
    private static Pattern qbPattern = Pattern.compile("^\\s*\\(.+\\)$");
    // private static Pattern dbPattern = Pattern.compile( "(?i)use
    // [a-zA-Z_0-9:\\/]+" );
    private static Pattern filterPattern = Pattern.compile("filter_fields=([a-zA-Z_0-9,]+)");
    private static Pattern aggFieldsPattern = Pattern.compile("agg_fields[\\d]?=(.+)");
    private static Pattern groupByPattern = Pattern
            .compile("(?i)(?:^|\\s+)(GROUPBY\\s+|SUM\\s+|MAX\\s+|MIN\\s+|MAXS\\s+|MINS\\s+|LIST\\s+)");
    private static Pattern limitPattern = Pattern.compile("limit=(\\d{1,})");
    private static Pattern collectPattern = Pattern.compile("collector=([a-zA-Z])");
    private static Pattern sortPattern = Pattern.compile("sort=([a-zA-Z0-9,:]+)");
    private static Pattern commentPattern = Pattern.compile("^#.*$");
    private static Pattern countPattern = Pattern.compile("^\\s*count\\s*$");
    private static Pattern paginPattern = Pattern.compile("^\\s*paginate\\s*$");
    private static Pattern debugPattern = Pattern.compile("^\\s*debug\\s*$");
    private static Pattern spoolPattern = Pattern.compile("^\\s*spool\\s+(.+)\\s*$");
    private static Pattern allPattern = Pattern.compile("^\\s*all\\s*$");

    private Pattern _nonSingleTermPattern = Pattern.compile("(\\w+\\s*:\\s*\\S+)|([*]\\s*:\\s*[*])+\\s*");
    // private Pattern _numericRangePattern = Pattern.compile(
    // "(\\w+\\s*:\\s*\\[\\d+\\s*[-]\\s*\\d+\\])+\\s*" );
    private Pattern _numericRangePattern = Pattern
            .compile("(\\w+\\s*:\\s*(\\[|\\{)\\s*\\d*\\s*[-]\\s*\\d*\\s*(\\]|\\}))\\s*");
    private IndexSearcher _searcher = null;
    private DirectoryReader _dreader = null;
    // private IndexReader reader = null;
    private IndexWriter _writer = null;
    private Analyzer _analyzer = null;
    private SearcherManager _searchManager = null;
    private String _indexDirPath = "./";
    private ScoreDoc _lastDocFound = null;
    private boolean _debugFlag = System.getProperty("debug") != null;
    // private QueryParser parser = null;

    private final Object readLock = new Object();

    // ------------------------------------------------------------------------
    public Searcher(String index_dir, Analyzer analyzer_) throws IOException {
        this(index_dir, analyzer_, false);
    }

    // -------------------------------------------------------------------------
    public Searcher(IndexWriter index_writer)
            throws IOException {
        _dreader = DirectoryReader.open(index_writer, true, false);
        _searchManager = new SearcherManager(_dreader, new SearcherFactory());
        _indexDirPath = index_writer.getDirectory().toString();
    }

    // -------------------------------------------------------------------------
    public Searcher(String index_dir, Analyzer analyzer_, boolean use_manager)
            throws IOException {
        _indexDirPath = index_dir;
        _analyzer = (analyzer_ != null ? analyzer_ : new StandardAnalyzer());
        ensureDir(index_dir);

        Directory indexDir = FSDirectory.open(FileSystems.getDefault().getPath(index_dir));
        _dreader = DirectoryReader.open(indexDir);

        if (!use_manager) {
            _searcher = new IndexSearcher(_dreader);
        } else {
            // IndexWriter writer_ = Indexer.createIndexWriter( index_dir, analyzer, false,
            // false );
            // writer = writer_;
            // System.out.println( writer.getConfig().getCodec() );
            // _searcher = new IndexSearcher( DirectoryReader.open( writer, false, false )
            // );
            // _searchManager = new SearcherManager( writer_, false, false, new
            // SearcherFactory());
            _searchManager = new SearcherManager(_dreader, new SearcherFactory());
        }
    }

    // -------------------------------------------------------------------------
    public String toString() {
        return String.format("SEARCHER ON %s", _indexDirPath);
    }

    // -------------------------------------------------------------------------
    private void ensureDir(String index_dir)
            throws IOException {
        Directory indexDir = FSDirectory.open(FileSystems.getDefault().getPath(index_dir));

        if (!DirectoryReader.indexExists(indexDir)) {
            Indexer idxr = new Indexer(index_dir, new StandardAnalyzer(), true);
            idxr.createDummyDoc();
            idxr.closeIndexWriter();
            idxr = null;
        }
    }

    // -------------------------------------------------------------------------
    public boolean dataChanged() throws IOException {
        return !_dreader.isCurrent();
    }

    // -------------------------------------------------------------------------
    private IndexSearcher getSearcher()
            throws IOException {

        if (!_dreader.isCurrent() /* && ! _nrtFlag */ ) {
            System.err.println("\n\n@@@ INDEX NOT IS CURRENT reloading reader for " + _indexDirPath);
            synchronized (readLock) {
                DirectoryReader newReader = DirectoryReader.openIfChanged(_dreader);

                // if write changes were commited this newReader will be non-null
                //
                if (newReader != null) {
                    _dreader.close();
                    _dreader = newReader;
                    if (_searchManager != null) {
                        _searchManager = new SearcherManager(_dreader, new SearcherFactory());
                    } else {
                        _searcher = new IndexSearcher(newReader);
                    }
                } else {
                    System.err.println("changes were not commited");
                }
            }
        }
        /**/
        return (_searchManager != null ? _searchManager.acquire() : _searcher);
    }

    // -------------------------------------------------------------------------
    private void releaseSearcher(IndexSearcher isr)
            throws IOException {
        if (_searchManager != null) {
            _searchManager.release(isr);
        }
    }

    // -------------------------------------------------------------------------
    // -- queryString in format: field_name:[long1-long2] OR
    // -- field_name:[-long2] OR
    // -- field_name:[long1-]
    // -- use curly brackets for exclusive queries
    // -------------------------------------------------------------------------
    private Query createSimpleLongRangeQuery(String queryString)
            throws ParseException {
        String[] ss = queryString.split(":");
        if (ss.length < 2) {
            throw new ParseException("RangeQuery: field name is missing");
        }

        boolean loExclusive = StringUtils.countMatches(queryString, "{") > 0,
                hiExclusive = StringUtils.countMatches(queryString, "}") > 0;

        String[] vals = ss[1].replaceAll("(\\[|\\]|\\{|\\})", "").split("[-]", -1);

        if (SimpleUtils.isEmpty(vals)) {
            throw new ParseException("RangeQuery: at least lower or upper values in range must be provided");
        }

        long lo = (StringUtils.isNumber(vals[0])) ? Long.parseLong(vals[0]) : Long.MIN_VALUE,
                hi = (StringUtils.isNumber(vals[1])) ? Long.parseLong(vals[1]) : Long.MAX_VALUE;

        if (loExclusive) {
            lo = SimpleUtils.addExact(lo, 1);
        }

        if (hiExclusive) {
            hi = SimpleUtils.addExact(hi, -1);
        }
        // System.out.printf( "LO %d HI %d\n", lo, hi );

        return LongPoint.newRangeQuery(ss[0], lo, hi);
    }

    /**/
    // -------------------------------------------------------------------------
    public Query createQuery(String fieldName, String queryString)
            throws ParseException {
        Query q = null;

        Matcher match = _nonSingleTermPattern.matcher(queryString);

        boolean nonSingleTermQueryUsed = match.find();
        boolean numericRangeQueryUsed = _numericRangePattern.matcher(queryString).find();

        if (numericRangeQueryUsed) {
            q = createSimpleLongRangeQuery(queryString);
        } else if (nonSingleTermQueryUsed) {
            // BooleanQuery.Builder bq = new BooleanQuery.Builder();
            QueryParser parser = new QueryParser(fieldName, _analyzer);
            parser.setAllowLeadingWildcard(true);
            q = parser.parse(queryString);
            // bq.add( q, BooleanClause.Occur.SHOULD );
            // String [] ss = queryString.split( ":", 2 );
            // bq.add( new TermQuery( new Term( ss[ 0 ], ss[ 1 ] ) ),
            // BooleanClause.Occur.SHOULD );
            // q = bq.build();
        } else {// System.out.println( "TERM QUERY " + fieldName + " " + queryString );
            q = new TermQuery(new Term(fieldName, queryString));
        }
        return q;
    }

    // -------------------------------------------------------------------------
    public DocumentCollector search(Query query,
            boolean sortByIdx, int n,
            Set<String> filterFields,
            DocumentCollector dc)
            throws IOException, ParseException {
        return search(query, sortByIdx ? Sort.INDEXORDER : null, n, filterFields, dc, false);
    }

    // -------------------------------------------------------------------------
    public DocumentCollector search(Query query,
            Sort srt, int n,
            Set<String> filterFields,
            DocumentCollector dc,
            boolean paginate)
            throws IOException, ParseException {
        long d1 = System.currentTimeMillis();

        if (srt == null) {
            srt = new Sort();
        }

        IndexSearcher isr = getSearcher();
        ScoreDoc[] hits = !(paginate && _lastDocFound != null) ? isr.search(query, n, srt, true).scoreDocs
                : isr.searchAfter(_lastDocFound, query, n, srt, true).scoreDocs;

        if (_debugFlag) {
            System.out.printf("\nSearch completed in %s sec.\nTotal hits: %d\n",
                    DateDiffUtils.diffToString(d1, System.currentTimeMillis()), hits.length);
            d1 = System.currentTimeMillis();
        }

        if (dc != null) {
            dc.setNumberOfDocs(hits.length);
        } else {
            dc = new SimpleDocumentCollector(hits.length, filterFields);
        }

        for (int i = 0; i < hits.length; i++) {
            Document doc = isr.doc(hits[i].doc);
            Field field = new StringField("score", String.valueOf(hits[i].score), Field.Store.NO);
            doc.add(field);

            dc.collect(doc);
        }

        _lastDocFound = hits.length > 0 && paginate ? hits[hits.length - 1] : null;
        if (_debugFlag) {
            System.out.printf("\n\nCollection completed in %s sec.\n",
                    DateDiffUtils.diffToString(d1, System.currentTimeMillis()));
        }
        releaseSearcher(isr);
        return dc;
    }

    // -------------------------------------------------------------------------
    public int count(Query query)
            throws IOException, ParseException {
        return getSearcher().count(query);
    }

    // -------------------------------------------------------------------------
    // -- fields are colon separated strings format field_name:field_type
    // -- valid types are int, float, string
    // -------------------------------------------------------------------------
    public static Sort createSort(String... fields) {
        SortField[] sfa = new SortField[fields.length];

        for (int i = 0, k = fields.length; i < k; i++) {
            String f = fields[i];
            // System.out.println( "---> " + f );
            final String[] ss = f.split(":");
            String n = ss[0],
                    t = null;

            SortField.Type sft = SortField.Type.STRING;

            if (ss.length > 1) {
                t = ss[1].trim();
                if (t.equalsIgnoreCase("int")) {
                    sft = SortField.Type.INT;
                } else if (t.equalsIgnoreCase("float")) {
                    sft = SortField.Type.DOUBLE;
                }
            }
            // System.out.println( sft );
            sfa[i] = new SortField(n, sft);
        }

        return new Sort(sfa);
    }

    // -------------------------------------------------------------------------
    public Map<String, String> getStats()
            throws IOException {
        IndexSearcher isr = getSearcher();
        IndexReader rd = isr.getIndexReader();
        Map<String, String> m = new HashMap<String, String>();
        m.put("total", String.valueOf(rd.numDocs()));
        m.put("deleted", String.valueOf(rd.numDeletedDocs()));
        return m;
    }

    // -------------------------------------------------------------------------
    public void close()
            throws IOException {
        getSearcher().getIndexReader().close();
        if (_writer != null) {
            _writer.close();
        }
        _searchManager = null;
    }

    // -------------------------------------------------------------------------
    private static void spool(String[] data, String path) throws IOException {
        File outputFile = new File(path);
        String timeStamp = SimpleUtils.dateToString(new Date(), "yyyyMMddHHmmss");

        if (outputFile.getParentFile() != null) {
            outputFile.getParentFile().mkdirs();
        }

        Writer w = new BufferedWriter(
                new OutputStreamWriter(
                        new FileOutputStream(path + timeStamp + ".txt", true),
                        Charset.forName("UTF-8").newEncoder()));

        for (String s : data) {
            w.write(s + "\n");
        }
        w.close();
    }

    // -------------------------------------------------------------------------
    // -- takes multiline string buffer where each line contains a query or option
    // -- the only options so far are use, limit and #
    // -------------------------------------------------------------------------
    public SimpleImmutableEntry<Integer, String[]> runCompoundQuery(String input)
            throws IOException, ParseException {
        return runCompoundQuery(input.split("\n"), null);
    }

    // -------------------------------------------------------------------------
    public SimpleImmutableEntry<Integer, String[]> runCompoundQuery(String input, DocumentCollector dc)
            throws IOException, ParseException {
        return runCompoundQuery(input.split("\n"), dc);
    }

    // -------------------------------------------------------------------------
    public SimpleImmutableEntry<Integer, String[]> runCompoundQuery(String[] params)
            throws IOException, ParseException {
        return runCompoundQuery(params, null);
    }

    // -------------------------------------------------------------------------
    public SimpleImmutableEntry<Integer, String[]> runCompoundQuery(String[] params, DocumentCollector dc)
            throws IOException, ParseException {
        Matcher matcher = null;
        String spoolPath = null;
        Set<String> filterFields = null;
        Sort srt = null;
        int limit = 20;
        boolean aggregateFlag = false,
                countFlag = false,
                useSrchFlag = false,
                debugFlag = false,
                spoolFlag = false,
                allFlag = false,
                paginateFlag = false;

        Map<String, Query> queries = new HashMap<String, Query>();
        Searcher searcher = null;
        Map<String, String> aggMap = new HashMap<String, String>();

        int cnt = 0;

        for (String p : params) {
            if (StringUtils.isBlank(p)) {
                continue;
            }

            matcher = commentPattern.matcher(p);

            if (matcher.find()) {
                continue;
            }

            matcher = allPattern.matcher(p);

            if (matcher.find()) {
                allFlag = true;
                continue;
            }

            matcher = debugPattern.matcher(p);

            if (matcher.find()) {
                debugFlag = true;
                continue;
            }

            matcher = spoolPattern.matcher(p);

            if (matcher.find()) {
                spoolFlag = true;
                spoolPath = matcher.group(1);
                // System.out.println( "###> " + spoolPath );
                continue;
            }

            matcher = countPattern.matcher(p);

            if (matcher.find()) {
                countFlag = true;
                continue;
            }

            matcher = sortPattern.matcher(p);

            if (matcher.find()) {
                srt = createSort(matcher.group(1).trim().split(","));
                continue;
            }

            matcher = paginPattern.matcher(p);

            if (matcher.find()) {
                paginateFlag = true;
                continue;
            }

            matcher = filterPattern.matcher(p);

            if (matcher.find()) {
                filterFields = new LinkedHashSet<String>(Arrays.asList(matcher.group(1).trim().split(",")));
                // System.out.println( "###> " + filterFields);
                continue;
            }

            matcher = aggFieldsPattern.matcher(p);

            if (matcher.find()) {
                // System.out.println( "AGG ###> " + matcher.group( 0 ) );
                aggregateFlag = true;
                aggMap.put("aggFields" + cnt, matcher.group(1));
                ++cnt;
                continue;
            }

            matcher = groupByPattern.matcher(p);

            if (matcher.find()) {
                // System.out.println( "GROUP BY ###> " + matcher.group( 0 ) );
                aggregateFlag = true;
                dc = new MFADocumentCollector(p);
                continue;
            }

            matcher = limitPattern.matcher(p);

            if (matcher.find()) {
                limit = Integer.parseInt(matcher.group(1));
                // System.out.println( "###> " + matcher.group( 1 ) );
                continue;
            }

            matcher = collectPattern.matcher(p);

            if (matcher.find()) {
                if (aggregateFlag) {
                    continue;
                }

                String t = matcher.group(1);

                if ("p".equalsIgnoreCase(t)) {
                    dc = new PlainTextCollector(100, filterFields);
                } else if ("c".equalsIgnoreCase(t)) {
                    filterFields = new HashSet<String>();
                    filterFields.add("content");
                    dc = new ContentOnlyCollector(100, filterFields);
                }
                // System.out.println( "###> " + matcher.group( 1 ) );
                continue;
            }

            matcher = qbPattern.matcher(p);

            if (matcher.find()) {
                queries.put(StringUtils.randomAlphaNumeric(5, SimpleUtils.PASSWORD_ALPHABET),
                        QueryBuilder.build(p));
                continue;
            }

            final String[] a = p.split("=");

            if (a.length > 1) {
                queries.put(a[0], createQuery(a[0], a[1]));
            }
        }

        if (aggregateFlag && !(dc instanceof MFADocumentCollector)) {// System.out.println( "AGG MAP " + aggMap );
            dc = new ChainAggregateDocumentCollector(30000, aggMap);
        }

        if (debugFlag) {
            System.err.println("QUERIES " + queries);
            for (Map.Entry<String, Query> e : queries.entrySet()) {
                System.err.printf("%s -> %s\n", e.getKey(), e.getValue().getClass().getName());
                // System.err.println( SimpleUtils.toString( e.getValue() ) );
            }
            System.err.println("!!! COLLECTOR " + dc);
        }

        cnt = 0;

        String[] data = new String[] { "" };

        try {
            BooleanQuery.Builder bq = new BooleanQuery.Builder();

            for (Map.Entry<String, Query> q : queries.entrySet()) {
                bq.add(q.getValue(), BooleanClause.Occur.MUST);
            }

            if (searcher == null) {
                searcher = this;
            }

            cnt = searcher.count(bq.build());

            if (allFlag || aggregateFlag) {
                limit = cnt;
            }

            if (limit < 1) {
                limit = 20;
            }
            ;

            if (spoolFlag) {
                if (dc == null) {
                    dc = new SimpleDocumentCollector(100, filterFields);
                }
                String timeStamp = SimpleUtils.dateToString(new Date(), "yyyyMMddHHmmss");
                dc.setOutStream(SimpleUtils.pathToOutStream(spoolPath + "_" + timeStamp + ".txt", false));
                dc.setRecSeparator("\n");
            }

            if (!countFlag) {
                data = searcher.search(bq.build(), srt, limit, filterFields, dc, paginateFlag).getData();
            } else {
                data = new String[] { String.format("{\"count\":\"%d\"}", cnt) };
            }

            if (spoolFlag) {
                // spool( data, spoolPath );
                data = new String[] { "data spooled into " + spoolPath };
            }

            if (aggregateFlag) {
                cnt = data.length;
            }

            if (useSrchFlag) {
                searcher.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (dc != null) {
                dc.closeOutStream();
            }
        }
        SimpleImmutableEntry<Integer, String[]> retVal = new SimpleImmutableEntry<Integer, String[]>(cnt, data);

        return retVal;
    }

    // -------------------------------------------------------------------------
    private String[] processInteractiveInput(String input)
            throws IOException, ParseException {
        Date d1 = new java.util.Date();
        SimpleImmutableEntry<Integer, String[]> r = runCompoundQuery(input);
        int[] diff = DateDiffUtils.splitMillis(new Date().getTime() - d1.getTime());
        String[] a = Arrays.copyOf(r.getValue(), r.getValue().length + 1);
        a[a.length - 1] = String.format("\nResults found: %d\nRun completed in %d:%d:%d.%d sec.",
                r.getKey(), diff[0], diff[1], diff[2], diff[3]);
        return a;
    }

    // -------------------------------------------------------------------------
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("USAGE: Searcher path_to_index_dir");
            System.exit(-1);
        }

        Searcher searcherRef = null;

        try {
            final Searcher searcher = new Searcher(args[0], null, true);
            searcherRef = searcher;
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    if (searcher != null) {
                        try {
                            searcher.close();
                        } catch (Exception e) {
                            System.out.printf("Closing searcher: %s\n", e);
                        }
                    }
                    System.out.println(Searcher.class.getName() + " exited!");
                }
            });

            System.out.println("\nSTATS: " + searcher.getStats());

            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

            String input;

            System.out.print("\nEnter field and query separated by colon\n" +
                    "Terminate your query by \".\" on separate line\n" +
                    "Example\ntitle:\"The Right Way\" AND text:go\n.\n--> ");

            StringBuilder sb = new StringBuilder();

            while ((input = br.readLine()) != null) {
                try {
                    if (StringUtils.isBlank(input)) {
                        System.out.print("\n--> ");
                        continue;
                    }

                    if (!".".equals(input)) {
                        sb.append(input).append("\n");
                    } else {
                        String[] results = searcher.processInteractiveInput(sb.toString());
                        // System.out.println( sb.toString() );

                        sb.delete(0, sb.length());

                        System.out.println("\nRESULTS\n");

                        for (String s : results) {
                            if (s == null) {
                                continue;
                            }
                            System.out.println(s);
                        }
                        System.out.print("\n--> ");
                    }
                } catch (Exception ee) {
                    sb.delete(0, sb.length());
                    ee.printStackTrace();
                }

            }
        } catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
        } finally {
            try {
                System.out.println("\nclosing searcher");
                searcherRef.close();
            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }
}