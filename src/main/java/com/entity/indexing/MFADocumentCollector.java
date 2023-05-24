package com.entity.indexing;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import org.apache.lucene.document.*;
import static com.entity.util.StringUtils.*;
import com.entity.util.*;

// Multifield Aggregate Document Collector
//
public class MFADocumentCollector extends DocumentCollector {
    private static final int GR = 0, SM = 1, MX = 2, MN = 3, MS = 4, MZ = 5, LS = 6;
    private static Pattern gbPattern = Pattern.compile("(?i)(?:^|\\s+)(GROUPBY)\\s+(\\w+\\s*(?:,\\s*\\w+)*)+?"),
            smPattern = Pattern.compile("(?i)(?:^|\\s+)(SUM)\\s+(\\w+\\s*(?:,\\s*\\w+)*)+?"),
            mxPattern = Pattern.compile("(?i)(?:^|\\s+)(MAX)\\s+(\\w+\\s*(?:,\\s*\\w+)*)+?"),
            mnPattern = Pattern.compile("(?i)(?:^|\\s+)(MIN)\\s+(\\w+\\s*(?:,\\s*\\w+)*)+?"),
            msPattern = Pattern.compile("(?i)(?:^|\\s+)(MAXS)\\s+(\\w+\\s*(?:,\\s*\\w+)*)+?"),
            mzPattern = Pattern.compile("(?i)(?:^|\\s+)(MINS)\\s+(\\w+\\s*(?:,\\s*\\w+)*)+?"),
            ftPattern = Pattern.compile("(?i)(?:^|\\s+)(FILTERBY)\\s+(\\w+\\:\\w+s*(?:,\\s*\\w+:\\w+)*)+?"),
            lsPattern = Pattern.compile("(?i)(?:^|\\s+)(LIST)\\s+(\\w+\\s*(?:,\\s*\\w+)*)+?");

    private static final Pattern[] REGEXES = new Pattern[] { gbPattern, smPattern, mxPattern, mnPattern, msPattern,
            mzPattern, ftPattern, lsPattern };

    private static final int[] OPS = new int[] { GR, SM, MX, MN, MS, MZ, LS };

    private static final String DEFAULT_KEY = "_";

    private Map<String, Bag> _bags = new HashMap<String, Bag>();
    private Map<String, String[]> _commands = new HashMap<String, String[]>();
    OutputStream _os = null;

    // -------------------------------------------------------------------------
    public MFADocumentCollector(String pCommand) {
        super(10, null);
        setup(pCommand);
    }

    // -------------------------------------------------------------------------
    public void collect(Document pDoc) {
        Bag bag = null;
        String[] filterFields = _commands.get("FILTERBY");

        if (SimpleUtils.isNotEmpty(filterFields) && !filterApplied(pDoc, filterFields)) {
            return;
        }

        for (int op : OPS) {
            String[] fieldNames = null;
            String key = null;

            switch (op) {
                case GR:
                    bag = fieldsToBag(pDoc, _commands.get("GROUPBY"));
                    bag._cnt += 1;
                    break;
                case SM:
                    if ((fieldNames = _commands.get("SUM")) == null) {
                        continue;
                    }
                    sum(pDoc, fieldNames, bag);
                    break;
                case MX:
                    if ((fieldNames = _commands.get("MAX")) == null) {
                        continue;
                    }
                    minMax(pDoc, fieldNames, bag, true);
                    break;
                case MN:
                    if ((fieldNames = _commands.get("MIN")) == null) {
                        continue;
                    }
                    minMax(pDoc, fieldNames, bag, false);
                    break;
                case MS:
                    if ((fieldNames = _commands.get("MAXS")) == null) {
                        continue;
                    }
                    minMaxS(pDoc, fieldNames, bag, true);
                    break;
                case MZ:
                    if ((fieldNames = _commands.get("MINS")) == null) {
                        continue;
                    }
                    minMaxS(pDoc, fieldNames, bag, false);
                    break;
                case LS:
                    if ((fieldNames = _commands.get("LIST")) == null) {
                        continue;
                    }
                    list(pDoc, fieldNames, bag);
                    break;
            }
        }
    }

    // -------------------------------------------------------------------------
    public Object getResultSet() {
        return _bags;
    }

    // -------------------------------------------------------------------------
    public String[] getData() {
        String[] a = new String[_bags.size()],
                g = _commands.get("GROUPBY");
        StringBuilder sb = new StringBuilder();

        int j = 0;

        for (Map.Entry<String, Bag> e : _bags.entrySet()) {
            final String key = e.getKey();
            final Bag val = e.getValue();

            String[] ss = key.split("[.]");

            // for( String s : ss ){ out.println( "--> " + s ); }
            sb.append(String.format("{\"%s\":\"%s\"", g[0], ss[0]));

            for (int i = 1, k = g.length; i < k; i++) {
                sb.append(',').append(String.format("\"%s\":\"%s\"", g[i], ss[i]));
            }
            sb.append(',').append(val.toString()).append('}');

            a[j++] = sb.toString();
            sb.delete(0, sb.length());
        }
        Arrays.sort(a);

        if (_os != null) {
            try {
                byte[] sep = _recSeparator.getBytes();
                for (String s : a) {
                    _os.write(s.getBytes("UTF-8"));
                    _os.write(sep);
                }
            } catch (Exception e) {
                System.err.println(e);
            }
        }
        return a;
    }

    // -------------------------------------------------------------------------
    private boolean filterApplied(Document doc, String[] filterFields) {
        for (String f : filterFields) {
            String[] ss = f.split(":"),
                    vals = doc.getValues(ss[0]);
            String z = ss[1];
            for (String v : vals) {
                if (z.compareTo(v) == 0) {
                    return true;
                }
            }
        }
        return false;
    }

    // -------------------------------------------------------------------------
    private static void list(Document doc, String[] fields, Bag bag) {
        for (String f : fields) {
            String[] vals = doc.getValues(f);

            if (SimpleUtils.isEmpty(vals)) {
                continue;
            }

            if (bag._m.get(f) == null) {
                bag.newAggVal(f);
            }
            if (bag._m.get(f)._ls == null) {
                bag._m.get(f)._ls = new TreeSet<String>();
            }

            Set<String> l = bag._m.get(f)._ls;
            l.addAll(Arrays.asList(vals));
        }
    }

    // -------------------------------------------------------------------------
    private static void minMax(Document doc, String[] fields, Bag bag, boolean getMax) {
        for (String f : fields) {
            String[] vals = doc.getValues(f);
            double val = 0.0D, m = 0.0D;

            if (SimpleUtils.isEmpty(vals)) {
                continue;
            }

            for (int i = 0, k = vals.length; i < k; i++) {
                String v = vals[i];

                if (isNumber(v)) {
                    double d = Double.parseDouble(v);

                    if (i == 0) {
                        val = d;
                        continue;
                    }
                    if (getMax) {
                        val = val < d ? d : val;
                    } else {
                        val = val > d ? d : val;
                    }
                }
            }

            m = val;

            if (bag._m.get(f) == null) {
                bag.newAggVal(f);
            }

            Object obj = getMax ? bag._m.get(f)._max : bag._m.get(f)._min;

            if (obj != null && obj instanceof Number) {
                m = ((Number) obj).doubleValue();
            }

            if (getMax) {
                bag._m.get(f)._max = Double.valueOf(m > val ? m : val);
            } else {
                bag._m.get(f)._min = Double.valueOf(m < val ? m : val);
            }
        }
    }

    // -------------------------------------------------------------------------
    private static void minMaxS(Document doc, String[] fields, Bag bag, boolean getMax) {
        for (String f : fields) {
            String[] vals = doc.getValues(f);
            String val = null;

            if (SimpleUtils.isEmpty(vals)) {
                continue;
            }

            if (bag._m.get(f) == null) {
                bag.newAggVal(f);
            }

            Object m = getMax ? bag._m.get(f)._max : bag._m.get(f)._min;

            if (m == null) {
                m = vals[0];
            }

            if (getMax) {
                bag._m.get(f)._max = StringUtils.max(StringUtils.max(vals), m.toString());
            } else {
                bag._m.get(f)._min = StringUtils.min(StringUtils.min(vals), m.toString());
            }
        }
    }

    // -------------------------------------------------------------------------
    private static void sum(Document doc, String[] fields, Bag bag) {
        for (String f : fields) {
            String[] vals = doc.getValues(f);

            if (SimpleUtils.isEmpty(vals)) {
                continue;
            }

            if (bag._m.get(f) == null) {
                bag.newAggVal(f);
            }

            double d = bag._m.get(f)._sum,
                    a = bag._m.get(f)._avg,
                    c = bag._cnt;
            for (String v : vals) {
                if (isNumber(v)) {
                    d += Double.valueOf(v).doubleValue();
                    a = d / c;
                }
            }
            bag._m.get(f)._sum = d;
            bag._m.get(f)._avg = a;
        }
    }

    // -------------------------------------------------------------------------
    private Bag fieldsToBag(Document doc, String[] fieldNames) {
        String[] fnames = SimpleUtils.isNotEmpty(fieldNames) ? fieldNames : new String[] { DEFAULT_KEY };

        // for( String s : fnames ){ out.println( "--> " + s ); }
        Bag b = null;
        Map<String, String> m = new TreeMap<String, String>();
        StringBuilder sb = new StringBuilder(join(doc.getValues(fnames[0]), "."));

        for (int i = 1, k = fnames.length; i < k; i++) {
            final String f = fnames[i],
                    z = join(doc.getValues(f), "_");
            // System.out.printf( "F %s Z %s %d %d\n", f, z, i, k );
            sb.append('.').append(z);
            m.put(f, z);
        }

        String key = sb.toString();
        // System.out.println( "KEY " + key );
        // System.out.println( "MAP " + m );

        if (StringUtils.isBlank(key)) {
            key = DEFAULT_KEY;
        }

        if (!_bags.containsKey(key)) {
            b = new Bag();
            _bags.put(key, b);
        } else {
            b = _bags.get(key);
        }

        return b;
    }

    // -------------------------------------------------------------------------
    private void setup(String cmd) {
        for (Pattern p : REGEXES) {
            _commands.putAll(parseCommand(cmd, p));
        }

        if (_commands.get("GROUPBY") == null) {
            _commands.put("GROUPBY", new String[] { DEFAULT_KEY });
        }
        // SimpleUtils.printParameterMap( _commands );
    }

    // -------------------------------------------------------------------------
    private static Map<String, String[]> parseCommand(String cmd, Pattern pat) {
        Map<String, String[]> result = new HashMap<String, String[]>();
        Matcher match = pat.matcher(cmd);

        // out.println( match );

        if (match.find()) {
            String[] ss = match.group(2).replaceAll("\\s+", "").split(",");
            List<String> l = new ArrayList<String>(Arrays.asList(ss));
            SimpleUtils.removeDups(l);
            result.put(match.group(1).toUpperCase(), l.toArray(new String[l.size()]));
        }
        return result;
    }

    // -------------------------------------------------------------------------
    public Bag newBag() {
        return new Bag();
    }

    // =========================================================================
    private class Bag implements Serializable {
        private static final long serialVersionUID = 362498820763181261L;

        private long _cnt = 0L;
        private Map<String, AggVal> _m = new HashMap<String, AggVal>();

        public void newAggVal(String field) {
            _m.put(field, new AggVal());
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();

            int i = 0, k = _m.size();

            if (k < 1) {
                sb.append(String.format("\"cnt\":%d ", _cnt));
            }

            for (Map.Entry<String, AggVal> e : _m.entrySet()) {
                final AggVal v = e.getValue();

                sb.append(String.format("\"%s\":{", e.getKey()));
                sb.append(String.format("\"sum\":%.3f, ", v._sum));
                sb.append(String.format("\"cnt\":%d, ", _cnt));
                sb.append(String.format("\"avg\":%.3f ", v._avg));
                if (v._min != null) {
                    sb.append(',').append(formatVal("min", v._min));
                }
                if (v._max != null) {
                    sb.append(',').append(formatVal("max", v._max));
                }
                if (v._ls != null) {
                    sb.append(String.format(", \"list\":[\"%s\"]", join(v._ls, "\",\"")));
                }
                sb.append("}");
                if (i + 1 < k) {
                    sb.append(", ");
                }
                i++;
            }
            return sb.toString();
        }

        private class AggVal {
            double _sum = 0.0D, _avg = 0.0D;
            Object _max, _min;
            Set<String> _ls;
        }
    }

    // --------------------------------------------------------------------------
    private String formatVal(String name, Object val) {
        if (val instanceof Number) {
            return String.format("\"%s\":%.3f", name, ((Number) val).doubleValue());
        } else {
            return String.format("\"%s\":\"%s\"", name, val.toString());
        }
    }

    // --------------------------------------------------------------------------
    public void setOutStream(OutputStream pOs) {
        _os = pOs;
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

/*
 * 
 * (+ (* account_id:000000302660WDA) txn_date:[20150101 TO 20150401} )
 * GROUPBY txn_date SUM txn_amount_orig FILTERBY txn_month:201502,
 * txn_date:20150315
 * .
 * 
 * GROUPBY f1,f2 SUM f1,f2 AVG f1,f2 COUNT f1,f2 MIN f1,f2, MAX f1,f2 LIST f1,f2
 * SUM f3 groupby f2,f1, f3 MAX f5 MIN f6,f5 count f8 List f9, f8
 */
