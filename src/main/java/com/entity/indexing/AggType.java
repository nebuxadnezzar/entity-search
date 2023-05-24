package com.entity.indexing;

import java.util.*;
import org.apache.lucene.index.*;
import com.entity.util.*;

public enum AggType {
    GROUPBY {
        @Override
        public Object aggregate(Object oldValue, IndexableField[] newValues, DocValuesType lastTypeUsed) {
            return oldValue;
        }

        public Object combine(Object val1, Object val2) {
            return null;
        }
    },
    COUNTBY {
        /*
         * @Override
         * public Object aggregate( Object oldValue, IndexableField [] newValues,
         * DocValuesType lastTypeUsed )
         * {
         * int cnt = 0;
         * if( oldValue == null )
         * { return cnt; }
         * String val = oldValue.toString();
         * 
         * for( IndexableField f : newValues )
         * {
         * String s = fieldValue( f ).toString();
         * if( s.compareTo( val ) == 0 )
         * { ++cnt; }
         * }
         * return new Integer( cnt );
         * }
         */

        @Override
        public Object aggregate(Object oldValue, IndexableField[] newValues, DocValuesType lastTypeUsed) {
            if (oldValue == null) {
                return 1;
            }

            return Integer.valueOf(((Integer) oldValue).intValue() + 1);
        }

        public Object combine(Object val1, Object val2) {
            if (val1 == null && val2 == null) {
                throw new IllegalArgumentException("COUNTBY requires at least one non-empty value");
            }
            if (val1 == null) {
                val1 = Integer.valueOf(0);
            } else if (val2 == null) {
                val2 = Integer.valueOf(0);
            }

            if (!(val1 instanceof Number && val2 instanceof Number)) {
                throw new IllegalArgumentException("COUNTBY requires both values to be numbers");
            }

            int d1 = ((Integer) val1).intValue(),
                    d2 = ((Integer) val2).intValue();
            return Integer.valueOf(d1 + d2);
        }
    },
    LEN {
        @Override
        public Object aggregate(Object oldValue, IndexableField[] newValues, DocValuesType lastTypeUsed) {
            int len = 0;
            if (oldValue != null) {
                len = ((Integer) oldValue).intValue();
            }
            return Integer.valueOf(len + newValues.length);
        }

        public Object combine(Object val1, Object val2) {// System.out.printf( "VAL1 %s VAL2 %s \n", ( val1 != null ?
                                                         // val1.toString() : "null" ), ( val2 != null ? val2.toString()
                                                         // : "null" ) );

            if (val1 == null && val2 == null) {
                throw new IllegalArgumentException("LEN requires at least one non-empty value");
            }
            if (val1 == null) {
                val1 = Integer.valueOf(0);
            } else if (val2 == null) {
                val2 = Integer.valueOf(0);
            }

            if (!(val1 instanceof Number && val2 instanceof Number)) {
                throw new IllegalArgumentException("LEN requires both values to be numbers");
            }

            int d1 = ((Integer) val1).intValue(),
                    d2 = ((Integer) val2).intValue();
            return Integer.valueOf(d1 + d2);
        }
    },
    SUM {
        @Override
        public Object aggregate(Object oldValue, IndexableField[] newValues, DocValuesType lastTypeUsed) {
            double val = 0.0;

            if (oldValue != null) {
                val = ((Double) oldValue).doubleValue();
            }
            for (int i = 0, k = newValues.length; i < k; i++) {
                final IndexableField f = newValues[i];

                if (f.numericValue() == null) {
                    throw new IllegalArgumentException("SUM requires numeric values");
                }

                val += f.numericValue().doubleValue();
            }
            return Double.valueOf(val);
        }

        // @Override
        public Object combine(Object val1, Object val2) {// System.out.printf( "VAL1 %s VAL2 %s \n", ( val1 != null ?
                                                         // val1.toString() : "null" ), ( val2 != null ? val2.toString()
                                                         // : "null" ) );
            if (val1 == null && val2 == null) {
                throw new IllegalArgumentException("SUM requires at least one non-empty value");
            }
            if (val1 == null) {
                val1 = Double.valueOf(0);
            } else if (val2 == null) {
                val2 = Double.valueOf(0);
            }

            if (!(val1 instanceof Number && val2 instanceof Number)) {
                throw new IllegalArgumentException("SUM requires both values to be numbers");
            }

            double d1 = ((Double) val1).doubleValue(),
                    d2 = ((Double) val2).doubleValue();
            return Double.valueOf(d1 + d2);
        }
    },
    MAX {
        @Override
        public Object aggregate(Object oldValue, IndexableField[] newValues, DocValuesType lastTypeUsed) {

            Object val = oldValueCheck(oldValue, newValues);

            for (int i = 0, k = newValues.length; i < k; i++) {
                final IndexableField f = newValues[i];

                switch (lastTypeUsed) {
                    case NUMERIC:
                        val = Math.min(((Number) val).doubleValue(), f.numericValue().doubleValue());
                        break;
                    default: {
                        String s1 = fieldValue(f).toString(),
                                s2 = val.toString();
                        val = cmp(s1, s2, true);
                    }
                }
            }
            return val;
        }

        public Object combine(Object val1, Object val2) {
            if (val1 == null && val2 == null) {
                throw new IllegalArgumentException("MAX requires at least one non-empty value");
            }
            if (val1 == null) {
                val1 = val2;
            } else if (val2 == null) {
                val2 = val1;
            }
            return cmp(val1.toString(), val2.toString(), true);
        }
    },
    MIN {
        @Override
        public Object aggregate(Object oldValue, IndexableField[] newValues, DocValuesType lastTypeUsed) {
            Object val = oldValueCheck(oldValue, newValues);

            for (int i = 0, k = newValues.length; i < k; i++) {
                final IndexableField f = newValues[i];

                switch (lastTypeUsed) {
                    case NUMERIC:
                        val = Math.min(((Number) val).doubleValue(), f.numericValue().doubleValue());
                        break;
                    default: {
                        String s1 = fieldValue(f).toString(),
                                s2 = val.toString();
                        val = cmp(s2, s1, false);
                    }
                }
            }
            return val;
        }

        public Object combine(Object val1, Object val2) {// System.out.printf( "VAL1 %s VAL2 %s \n", ( val1 != null ?
                                                         // val1.toString() : "null" ), ( val2 != null ? val2.toString()
                                                         // : "null" ) );
            if (val1 == null && val2 == null) {
                throw new IllegalArgumentException("MIN requires at least one non-empty value");
            }
            if (val1 == null) {
                val1 = val2;
            } else if (val2 == null) {
                val2 = val1;
            }
            return cmp(val2.toString(), val1.toString(), false);
        }
    },
    LIST {
        @SuppressWarnings("unchecked")
        public Object aggregate(Object oldValue, IndexableField[] newValues, DocValuesType lastTypeUsed) {
            Set<String> val = (TreeSet<String>) oldValue;

            if (val == null) {
                val = new TreeSet<String>();
            }

            for (IndexableField f : newValues) {
                String s = fieldValue(f).toString();
                val.add(s);
            }
            return val;
        }

        public Object combine(Object val1, Object val2) {
            return null;
        }
    };

    // -------------------------------------------------------------------------
    private static Object cmp(String s1, String s2, boolean returnMax) {
        Object val = null;
        if (s1 == null || s2 == null) {
            throw new IllegalArgumentException("cmp requires both values to be non-empty");
        }

        if (StringUtils.isNumber(s1) && StringUtils.isNumber(s2)) {
            double d1 = Double.parseDouble(s1),
                    d2 = Double.parseDouble(s2);
            val = returnMax ? Math.max(d1, d2) : Math.min(d1, d2);
        } else if (returnMax) {
            val = StringUtils.max(s1, s2);
        } else {
            val = StringUtils.min(s1, s2);
        }
        return val;
    }

    // -------------------------------------------------------------------------
    private static Object oldValueCheck(Object oldValue, IndexableField[] newValues) {
        if (oldValue != null) {
            return oldValue;
        }

        if (newValues.length < 1) {
            throw new RuntimeException("cannot perform aggregation on empty fields");
        }

        return fieldValue(newValues[0]);
    }

    // -------------------------------------------------------------------------
    private static Object fieldValue(IndexableField f) {
        if (f == null) {
            return "";
        }
        Object val = null;

        if (f.stringValue() != null) {
            val = f.stringValue();
            /* System.out.println( "GETTING STRING VALUE " + val ); */} else

        if (f.binaryValue() != null) {
            val = f.binaryValue().toString();
            /* System.out.println( "GETTING BINARY VALUE " + val ); */} else

        if (f.readerValue() != null) {
            val = f.readerValue().toString();
            /* System.out.println( "GETTING READER VALUE " + val ); */} else

        if (f.numericValue() != null) {
            val = f.numericValue();
            /* System.out.println( "GETTING NUMERIC VALUE " + val ); */}

        return val;
    }

    // -------------------------------------------------------------------------
    public abstract Object aggregate(Object oldValue, IndexableField[] newValues, DocValuesType lastTypeUsed);

    public abstract Object combine(Object val1, Object val2);
}
