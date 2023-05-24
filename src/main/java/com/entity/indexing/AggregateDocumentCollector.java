package com.entity.indexing;

/*
Lucene's documents are structured as following
{ "field_name":[val1, val2, ... , valN ] }

It will work as expected only if there is exactly ONE value in group by field
of each document in the collection.

In examples below account_name = GroupBy field and account_id = Count field

Good example:

    {"account_id":["000000055921WDA"],"account_name":["HSBC HONG KONG"]}
    {"account_id":["000000122912WDA","000000122932WDA"],"account_name":["HSBC HONG KONG"]}

for all other variations results will vary

Bad examples:

    {"account_id":["000000055921WDA"],"account_name":["HSBC HONG KONG","BERLIN"]}
    {"account_id":["000000122912WDA","000000122932WDA"],"account_name":["HSBC HONG KONG"]}
OR
    {"account_id":["000000055921WDA","000000122932WDA"],"account_name":["HSBC HONG KONG","BERLIN"]}
    {"account_id":["000000122912WDA"],"account_name":["HSBC HONG KONG"]}

OUTPUT:
    { groupByFieldVal1: { sum: num1, max: val11, min: val13 },
      groupByFieldVal2: { sum: num2, max: val21, min: val23 },
      groupByFieldValN: { sum: numN, max: valN,  min: valM } }

QUERY FORMAT:

    agg_fields0=account_id=GROUPBY;txn_amount_orig=SUM;source_txn_unique_id=LEN;txn_date=MIN;txn_date=MAX;000000122912WDA=FILTERBY

*/
import java.io.*;
import java.util.*;
import java.math.*;
import java.text.*;
import java.security.SecureRandom;
import org.apache.lucene.index.*;
import org.apache.lucene.document.*;
import com.entity.util.*;

public class AggregateDocumentCollector extends DocumentCollector {
    private static final String DEFAULT_GROUP_BY = "__totals__";

    private String[] _docs = null;

    private String _groupByField,
            _countByField,
            _sumField,
            _maxField,
            _minField,
            _lenField,
            _listField,
            _filterByVal;
    private DocValuesType _lastMaxType = null,
            _lastMinType = null;

    private static final DecimalFormat DF = new DecimalFormat("0.00");

    // private static final // is it used?
    // String [] _rowFields = new String[]{ "sum", "count", "max", "min" };

    private Set<AggType> _ops = new HashSet<AggType>();

    private Map<String, Map<String, Object>> _distinct = new HashMap<String, Map<String, Object>>();

    public AggregateDocumentCollector(int pNumOfDocs, Map<String, String> pFieldMap) {
        super(pNumOfDocs, null);

        AggType groupby = AggType.GROUPBY,
                countby = AggType.COUNTBY,
                sum = AggType.SUM,
                max = AggType.MAX,
                min = AggType.MIN,
                len = AggType.LEN,
                lst = AggType.LIST;

        if (pFieldMap == null || pFieldMap.size() < 1) {
            throw new RuntimeException("empty FieldMap");
        }

        String aggFields = pFieldMap.get("aggFields");

        if (StringUtils.isBlank(aggFields)) {
            throw new RuntimeException("aggFields property is empty");
        }

        String[] ss = StringUtils.normalize(aggFields)
                .replaceAll("\\s+", "")
                .split(";");

        for (String s : ss) {
            // System.out.println( "--> " + s );
            String[] zz = s.split("=", -1);

            if (zz.length < 2) {
                System.out.printf("Skipping invalid param %s\n", s);
                continue;
            }

            if (zz[1].equalsIgnoreCase(groupby.name())) {
                _groupByField = zz[0];
            } else if (zz[1].equalsIgnoreCase(countby.name())) {
                _countByField = zz[0];
                _ops.add(AggType.COUNTBY);
            } else if (zz[1].equalsIgnoreCase(sum.name())) {
                _sumField = zz[0];
                _ops.add(AggType.SUM);
            } else if (zz[1].equalsIgnoreCase(max.name())) {
                _maxField = zz[0];
                _ops.add(AggType.MAX);
            } else if (zz[1].equalsIgnoreCase(min.name())) {
                _minField = zz[0];
                _ops.add(AggType.MIN);
            } else if (zz[1].equalsIgnoreCase(len.name())) {
                _lenField = zz[0];
                _ops.add(AggType.LEN);
            } else if (zz[1].equalsIgnoreCase(lst.name())) {
                _listField = zz[0];
                _ops.add(AggType.LIST);
            } else if (zz[1].equalsIgnoreCase("FILTERBY")) {
                _filterByVal = zz[0];
            }

        }
        /*
         * System.err.printf(
         * "GROUP BY %s COUNTBY %s SUM %s MAX %s MIN %s LEN %s FILTER BY %s\n",
         * _groupByField, _countByField, _sumField, _maxField, _minField, _lenField,
         * _filterByVal );
         */
        if (!(StringUtils.isNotBlank(_groupByField) ||
                StringUtils.isNotBlank(_countByField) ||
                StringUtils.isNotBlank(_sumField) ||
                StringUtils.isNotBlank(_maxField) ||
                StringUtils.isNotBlank(_minField) ||
                StringUtils.isNotBlank(_listField) ||
                StringUtils.isNotBlank(_lenField))) {
            throw new RuntimeException(String.format("At least one aggregate field must be provided\n" +
                    "PARAM %s\n" +
                    "GROUP BY %s COUNTBY %s SUM %s MAX %s MIN %s LEN %s FILTER BY %s\n",
                    aggFields,
                    _groupByField, _countByField, _sumField, _maxField, _minField, _lenField, _filterByVal));
        }

        if (StringUtils.isBlank(_groupByField)) {
            _groupByField = DEFAULT_GROUP_BY; // new BigInteger( 130, new SecureRandom() ).toString( 32 );
            _distinct.put(_groupByField, new HashMap<String, Object>());
        }
    }

    // -------------------------------------------------------------------------
    public void setOutStream(OutputStream pOs) {
    }

    // -------------------------------------------------------------------------
    public void collect(Document pDoc) {
        // populate _distinct with keys, get them from _groupByField
        //
        String[] keys = pDoc.getValues(_groupByField);
        Set<String> groupByFields = new HashSet<String>();

        if (_filterByVal != null) {
            groupByFields.add(_filterByVal);
        } else {
            groupByFields.addAll(Arrays.asList(keys));
        }

        if (SimpleUtils.isEmpty(keys) && !_distinct.containsKey(DEFAULT_GROUP_BY)) {
            _distinct.put(DEFAULT_GROUP_BY, new HashMap<String, Object>());
        }

        if (_distinct.containsKey(DEFAULT_GROUP_BY)) {
            groupByFields.add(DEFAULT_GROUP_BY);
        }

        // if( _filterByVal != null && ! key.equalsIgnoreCase( _filterByVal ) )
        // { groupByFields; }
        int cnt = 0;
        for (String key : keys) { // System.out.println( "KEY FIELD - " + (cnt++ ) + " " + key );
                                  // System.out.printf( "FILTER %s KEY %s\n", _filterByVal, key );

            if (!_distinct.containsKey(key)) {
                _distinct.put(key, new HashMap<String, Object>());
            }
        }

        for (AggType t : _ops) {
            String fieldName = null, rowField = null;
            DocValuesType lastType = null;
            Object val = null;

            switch (t) {
                case SUM:
                    fieldName = _sumField;
                    rowField = "sum";
                    break;
                case MIN:
                    fieldName = _minField;
                    rowField = "min";
                    break;
                case MAX:
                    fieldName = _maxField;
                    rowField = "max";
                    break;
                case LEN:
                    fieldName = _lenField;
                    rowField = "length";
                    break;
                case LIST:
                    fieldName = _listField;
                    rowField = "list";
                    break;
                case COUNTBY:
                    fieldName = _countByField;
                    rowField = "count";
                    break;
                // case GROUPBY: fieldName = _groupByField; break;
            }

            IndexableField[] newValues = pDoc.getFields(fieldName);

            if (SimpleUtils.isEmpty(newValues)) {
                continue;
            }

            if (t == AggType.MIN || t == AggType.MAX) {
                switch (t) {
                    case MIN:
                        if (_lastMinType == null && SimpleUtils.isNotEmpty(newValues)) {
                            _lastMinType = newValues[0].fieldType().docValuesType();
                        }
                        lastType = _lastMinType;
                        break;
                    case MAX:
                        if (_lastMaxType == null && SimpleUtils.isNotEmpty(newValues)) {
                            _lastMaxType = newValues[0].fieldType().docValuesType();
                        }
                        lastType = _lastMaxType;
                        break;
                }
            }

            Map<String, Object> row = null;
            Object oldVal = null;

            if (t == AggType.LIST) {

                for (String key : groupByFields) {
                    row = _distinct.get(key);
                    oldVal = row.get(rowField);
                    val = t.aggregate(oldVal, newValues, lastType);
                    row.put(rowField, val);
                }
            } else
            /*
             * if( t == AggType.COUNTBY )
             * {
             * for( String key : groupByFields )
             * {
             * row = _distinct.get( key );
             * oldVal = row.get( rowField );
             * val = t.aggregate( key, newValues, lastType );
             * //if( oldVal == null ){ oldVal = SimpleUtils.defaultObjVal( val ); }
             * row.put( rowField, t.combine( oldVal, val ) );
             * }
             * }
             * else
             */
            if (t == AggType.COUNTBY) {
                for (String key : groupByFields) {
                    row = _distinct.get(key);
                    oldVal = row.get(rowField);
                    val = t.aggregate(oldVal, newValues, lastType);
                    // if( oldVal == null ){ oldVal = SimpleUtils.defaultObjVal( val ); }
                    row.put(rowField, val);
                }
            } else {
                val = t.aggregate(null, newValues, lastType);

                for (String key : groupByFields) {
                    row = _distinct.get(key);
                    oldVal = row.get(rowField);
                    // if( oldVal == null ){ oldVal = fieldToString( newValues[0] ); }
                    row.put(rowField, t.combine(oldVal, val));
                }
            }

        }
    }

    // -------------------------------------------------------------------------
    public Object getResultSet() {
        return _distinct;
    }

    // -------------------------------------------------------------------------
    public String[] getData() {
        String prefix = "{\"" + _groupByField +
                "\":\"%s\",\"count\":\"%s\",\"len\":\"%s\",\"min\":\"%s\",\"max\":\"%s\",\"sum\":\"%s\",\"list\":%s}";
        int idx = 0, sz = _distinct.size();

        // System.out.println( "AGG RESULT " + _distinct );

        if (_distinct.get(DEFAULT_GROUP_BY) != null && _distinct.size() > 1) {
            sz++;
        }

        // check if we already generated this data and it's size did not change
        // meaning that data collector was not called twice
        //
        if (_docs != null && _docs.length > 0 && _docs.length == sz) {
            return _docs;
        }

        _docs = new String[sz];
        /**/
        for (Map.Entry<String, Map<String, Object>> e : _distinct.entrySet()) {
            if (_filterByVal != null && !e.getKey().equalsIgnoreCase(_filterByVal)) {
                continue;
            }

            Map<String, Object> row = (HashMap<String, Object>) e.getValue();

            _docs[idx++] = String.format(prefix,
                    e.getKey(),
                    defaultStrVal(row.get("count")),
                    defaultStrVal(row.get("length")),
                    defaultStrVal(row.get("min")),
                    defaultStrVal(row.get("max")),
                    formattedDoubleStr(row.get("sum")),
                    defaultListVal(row.get("list")));
        }
        // for( String s : docs )
        // { System.out.println( "+++> " + s ); }
        Map<String, Object> row = (HashMap<String, Object>) _distinct.get(DEFAULT_GROUP_BY);
        if (row != null && _distinct.size() > 1) {
            _docs[idx] = String.format(prefix,
                    "",
                    defaultStrVal(row.get("count")),
                    defaultStrVal(row.get("length")),
                    defaultStrVal(row.get("min")),
                    defaultStrVal(row.get("max")),
                    formattedDoubleStr(row.get("sum")),
                    defaultListVal(row.get("list")));

        }

        if (_docs.length > idx) {
            _docs = Arrays.copyOf(_docs, idx);
        } else {
            Arrays.sort(_docs);
        }
        // for( String s : _docs ){ System.err.println( "###> " + s ); }

        return _docs;
    }

    // -------------------------------------------------------------------------
    @SuppressWarnings("unchecked")
    private String defaultListVal(Object v) {
        return (v != null ? "[\"" + StringUtils.join((TreeSet<String>) v, "\",\"") + "\"]" : "[]");
    }

    // -------------------------------------------------------------------------
    private String defaultStrVal(Object v) {
        return (v != null ? v.toString() : "");
    }

    // -------------------------------------------------------------------------
    private String formattedDoubleStr(Object v) {
        if (v == null || !(v instanceof Double)) {
            return "";
        }

        return DF.format(((Double) v).doubleValue());
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
