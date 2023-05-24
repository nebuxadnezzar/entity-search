package com.entity.indexing;

import java.util.*;
import java.util.regex.*;
import org.apache.lucene.util.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause.*;
import org.apache.lucene.index.*;
import org.apache.lucene.document.LongPoint;
import static com.entity.util.StringUtils.*;
import com.entity.util.*;

enum BOOLEANCLAUSE {
    SHOULD, MUST, MUST_NOT, FUZZY
}

class QueryHelper {

    private static Pattern SRANGEPATTERN = Pattern.compile("[\\[{](\\s*\\w*\\s+)?TO(\\s+\\w*\\s*)?[\\]}]"),
            NRANGEPATTERN = Pattern.compile("[\\[{](\\s*\\w*\\s+)?[-](\\s+\\w*\\s*)?[\\]}]");
    BOOLEANCLAUSE _bc;
    List<String> _terms = new ArrayList<String>();
    List<QueryHelper> _helpers = new ArrayList<QueryHelper>();
    Map<String, Integer> _fuzzyOpts = null;

    public QueryHelper(BOOLEANCLAUSE bc, String op) {
        _bc = bc;

        if (_bc == BOOLEANCLAUSE.FUZZY) {
            _fuzzyOpts = resolveFuzzyOpts(op);
        }
    }

    // -------------------------------------------------------------------------
    public QueryHelper add(String term) {
        _terms.add(term);
        return this;
    }

    // -------------------------------------------------------------------------
    public QueryHelper add(QueryHelper qh) {
        _helpers.add(qh);
        return this;
    }

    // ------------------------------------------------------------------------
    public Query build() {
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        BooleanClause.Occur bc = _bc != BOOLEANCLAUSE.FUZZY ? Occur.valueOf(_bc.name()) : Occur.SHOULD;

        for (QueryHelper qh : _helpers) {
            bq.add(qh.build(), bc);
        }

        for (String t : _terms) {
            String[] ss = t.split(":", 2);

            if (ss.length < 2) {
                continue;
            }

            if ("*".equals(ss[0]) /* && "*".equals( ss[1] ) */ ) {
                bq.add(new MatchAllDocsQuery(), bc);
            } else if (ss[1].endsWith("*") && ss[1].length() > 1 && countMatches(ss[1], "*") == 1) {// out.println(
                                                                                                    // "Adding prefix
                                                                                                    // query" );
                String prefix = ss[1].replace("*", "");
                bq.add(new PrefixQuery(new Term(ss[0], prefix)), bc);
                if (bc == Occur.SHOULD) {
                    bq.add(new PrefixQuery(new Term(ss[0], prefix.toLowerCase())), bc);
                }
            } else if (ss[1].contains("*") || ss[1].contains("?") || ss[1].contains("\\")) {// out.println( "Adding
                                                                                            // wildcard query" );
                bq.add(new WildcardQuery(new Term(ss[0], ss[1])), bc);
                if (bc == Occur.SHOULD) {
                    bq.add(new WildcardQuery(new Term(ss[0], ss[1].toLowerCase())), bc);
                }
            } else if (_bc == BOOLEANCLAUSE.FUZZY) {// out.println( "1. Adding fuzzy query" );
                bq.add(createFuzzyQuery(ss[0], ss[1], _fuzzyOpts), Occur.SHOULD);
            } else if (ss[1].contains(" ")) {
                // Range queries
                //
                Matcher matcher = SRANGEPATTERN.matcher(ss[1]);
                if (matcher.find()) {
                    bq.add(createRangeQuery(ss[0], ss[1]), bc);
                    continue;
                }

                matcher = NRANGEPATTERN.matcher(ss[1]);
                if (matcher.find()) {
                    bq.add(createRangeQuery(ss[0], ss[1]), bc);
                    continue;
                }

                // Phrase query here
                //
                int slop = 0;
                if (ss[1].indexOf("~") > -1) {// out.println( "1. Adding phrase query" );
                    String[] zz = ss[1].split("~");
                    if (zz.length > 1 && StringUtils.isNumeric(zz[1])) {
                        slop = Integer.parseInt(zz[1]);
                    }

                    ss[1] = zz[0];
                }
                bq.add(new PhraseQuery(slop, ss[0], ss[1].split("\\s+")), bc);
                if (bc == Occur.SHOULD) {
                    bq.add(new PhraseQuery(slop, ss[0], ss[1].toLowerCase().split("\\s+")), bc);
                }
            } else if (ss[1].contains("~")) {// out.println( "2. Adding fuzzy query" );
                int edits = 0;

                String[] zz = ss[1].split("~");
                if (zz.length > 1 && StringUtils.isNumber(zz[1])) {
                    edits = floatToEdits(Float.parseFloat(zz[1]), zz[0].length());
                }
                ss[1] = zz[0];
                bq.add(new FuzzyQuery(new Term(ss[0], ss[1]), edits), Occur.SHOULD);
            } else {// out.println( "Adding term query" );
                bq.add(new TermQuery(new Term(ss[0], ss[1])), bc);
                if (bc == Occur.SHOULD) {
                    bq.add(new TermQuery(new Term(ss[0], ss[1].toLowerCase())), bc);
                }
            }
        }
        return bq.build();
    }

    // ------------------------------------------------------------------------
    public static Query createFuzzyQuery(String field, String value, Map<String, Integer> opts) {
        // out.println( "FUZZY OPTS " + opts );
        return new FuzzyQuery(new Term(field, value),
                opts.get("maxEdits"),
                opts.get("prefixLength"),
                opts.get("maxExpansion"),
                opts.get("transpositions") != 0 ? true : false);
    }

    // ------------------------------------------------------------------------
    public static Query createRangeQuery(String field, String query) {
        String q = query.replaceAll("\\s+", " ");
        Query z = null;

        boolean loExclusive = q.indexOf("{") > -1,
                hiExclusive = q.indexOf("}") > -1,
                isTermRange = q.matches("(?i).* TO .*");
        q = q.replaceAll("(\\[|\\]|\\{|\\})", "").trim();

        String[] vals = q.split("(?i)TO", -1);

        if (vals.length < 2) {
            vals = q.split("-", -1);
        }

        for (int i = 0, k = vals.length; i < k; i++) {
            vals[i] = vals[i].trim();
        }

        if (isTermRange) {
            String lo = isBlank(vals[0]) ? null : vals[0].trim(),
                    hi = isBlank(vals[1]) ? null : vals[1].trim();
            z = new TermRangeQuery(field,
                    lo != null ? new BytesRef(lo.getBytes()) : null,
                    hi != null ? new BytesRef(hi.getBytes()) : null,
                    !loExclusive, !hiExclusive);
        } else {
            long lo = (isNumeric(vals[0])) ? Long.parseLong(vals[0]) : Long.MIN_VALUE,
                    hi = (isNumeric(vals[1])) ? Long.parseLong(vals[1]) : Long.MAX_VALUE;

            if (loExclusive) {
                lo = SimpleUtils.addExact(lo, 1);
            }

            if (hiExclusive) {
                hi = SimpleUtils.addExact(hi, -1);
            }

            z = LongPoint.newRangeQuery(field, lo, hi);
        }
        // out.println( SimpleUtils.toString( z ) );
        return z;
    }

    // ------------------------------------------------------------------------
    // -- @param colon separated string of numbers
    // ------------------------------------------------------------------------
    private Map<String, Integer> resolveFuzzyOpts(String strOpts) {
        Map<String, Integer> m = new HashMap<String, Integer>();
        m.put("maxEdits", 2);
        m.put("prefixLength", 0);
        m.put("maxExpansion", 50);
        m.put("transpositions", 1);

        if (StringUtils.isBlank(strOpts)) {
            return m;
        }

        String[] ss = strOpts.split(":", -1);

        for (int i = 1, k = ss.length; i < k; i++) {
            if (!StringUtils.isNumeric(ss[i])) {
                continue;
            }

            String key = null;

            switch (i) {
                case 1:
                    key = "maxEdits";
                    break;
                case 2:
                    key = "prefixLength";
                    break;
                case 3:
                    key = "maxExpansion";
                    break;
                case 4:
                    key = "transpositions";
                    break;
            }

            m.put(key, Integer.valueOf(ss[i]));
        }

        return m;
    }

    // ------------------------------------------------------------------------
    public static int floatToEdits(float minimumSimilarity, int termLen) {
        if (minimumSimilarity >= 1.0F) {
            return (int) Math.min(minimumSimilarity, 2.0F);
        }
        if (minimumSimilarity == 0.0F) {
            return 0;
        }
        return Math.min((int) ((1.0D - minimumSimilarity) * termLen), 2);
    }

    // ------------------------------------------------------------------------
    public String toString() {
        return String.format("<<%s %s %s>>", _bc, _terms.toString(), _helpers.toString());
    }
}

public class QueryBuilder {
    private static BOOLEANCLAUSE resolveOp(String op) {
        if (op.equals("+")) {
            return BOOLEANCLAUSE.MUST;
        }

        if (op.equals("-")) {
            return BOOLEANCLAUSE.MUST_NOT;
        }

        if (op.equals("*")) {
            return BOOLEANCLAUSE.SHOULD;
        }

        if (op.matches("f(:\\d{1,2}){0,4}")) {
            return BOOLEANCLAUSE.FUZZY;
        }

        throw new RuntimeException("Invalid operand " + op);
    }

    // -------------------------------------------------------------------------
    public static Query build(String query) {
        if (isBlank(query)) {
            throw new IllegalArgumentException("Empty Query String");
        }

        int diff = countMatches(query, ")") - countMatches(query, "(");
        // System.out.printf( "LEFT DIFF %d RIGHT DIFF %d\n", countMatches( query, ")" )
        // ,countMatches( query, "(" ) );
        if (diff != 0) {
            throw new RuntimeException(String.format("Unmatched parentesis for query %s\ndiff=%d\n", query, diff));
        }

        Stack<QueryHelper> qt = new Stack<QueryHelper>();
        StringBuilder sb = new StringBuilder();

        byte[] tokens = query.replaceAll("(\n|\r)", " ").getBytes();
        boolean opflag = false;
        int offset = 0;

        // skip all chars before the first parenthesis
        //
        for (int k = tokens.length; offset < k; offset++) {
            if ((char) tokens[offset] == '(') {
                break;
            }
        }

        for (int i = offset, k = tokens.length; i < k; i++) {
            char t = (char) tokens[i];

            // System.out.println( "1. TOP OF THE STACK " + ( ! qt.isEmpty() ? qt.peek() :
            // "" ) );
            switch (t) {
                case '(':
                    opflag = true;
                    addToStack(qt, sb);
                    break;
                case ')':
                    if (qt.size() > 0) {
                        addToStack(qt, sb);
                        if (qt.size() > 1) {
                            QueryHelper qh = qt.pop();
                            qt.peek().add(qh);
                        }
                    }
                    break;
                case ' ':
                    if (sb.length() > 0) {
                        if (!opflag)
                            qt.peek().add(sb.toString());
                        else {
                            opflag = false;
                            String op = sb.toString().trim();
                            QueryHelper q = new QueryHelper(resolveOp(op), op);
                            qt.push(q);
                        }
                        sb.delete(0, sb.length());
                    }
                    break;
                case '"': {
                    int j = i + 1;

                    while (j < k) {
                        char z = (char) tokens[j];

                        if (z == '\\' && j + 1 < k && (char) tokens[j + 1] == '"') {
                            sb.append((char) tokens[j + 1]);
                            ++j;
                        } else if (z != '"') {
                            sb.append(z);
                        } else {
                            break;
                        }

                        ++j;
                    }
                    i = j;
                }
                    // System.out.println( "!!! " + sb );
                    break;
                case '{':
                case '[': {
                    sb.append(t);
                    int j = i;

                    while (j++ < k) {
                        char z = (char) tokens[j];
                        sb.append(z);
                        if (z == ']' || z == '}') {
                            break;
                        }
                        i = j;
                    }
                }
                    // System.out.println( "!!! " + sb );
                    break;
                default: {
                    sb.append(t);
                }
            }
            // System.out.println( "2. TOP OF THE STACK " + ( ! qt.isEmpty() ? qt.peek() :
            // "" ));
        }

        // System.out.println( qt );
        BooleanQuery.Builder bq = new BooleanQuery.Builder();

        while (!qt.isEmpty()) {
            bq.add(qt.pop().build(), Occur.SHOULD);
        }

        return bq.build();
    }

    // -------------------------------------------------------------------------
    private static void addToStack(Stack<QueryHelper> qt, StringBuilder sb) {
        if (sb.length() > 0 && !qt.isEmpty()) {
            qt.peek().add(sb.toString());
            sb.delete(0, sb.length());
        }
    }

    // -------------------------------------------------------------------------
    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.out.println("provide path to index directory");
            return;
        }

        Searcher searcher = new Searcher(args[0], null, false);

        java.io.BufferedReader br = new java.io.BufferedReader(new java.io.InputStreamReader(System.in));

        String input;
        StringBuilder sb = new StringBuilder();

        while ((input = br.readLine()) != null) {
            try {
                if (isBlank(input)) {
                    System.out.print("\n--> ");
                    continue;
                }

                if (!".".equals(input)) {
                    sb.append(input).append("\n");
                } else {
                    // System.out.println( sb.toString() );
                    Query q = build(sb.toString());
                    System.out.println("QUERY " + q);

                    String[] results = searcher.runCompoundQuery(sb.toString()).getValue();
                    int cnt = searcher.count(q);

                    sb.delete(0, sb.length());

                    System.out.println("\nRESULTS\n");

                    for (String s : results) {
                        if (s == null) {
                            continue;
                        }
                        System.out.println(s);
                    }
                    System.out.printf("\n(%d)==> ", cnt);
                }
            } catch (Exception ee) {
                ee.printStackTrace();
                sb.delete(0, sb.length());
            }

        }
    }
}