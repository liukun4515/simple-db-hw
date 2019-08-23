package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private TupleDesc tupleDesc;
    private Map map;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        Type[] types;
        int i = 0;
        // (group value,aggregate value)
        // the aggregate value is string type
        if (this.gbfield != Aggregator.NO_GROUPING) {
            types = new Type[2];
            types[i] = gbfieldtype;
            i++;
        } else {
            types = new Type[1];
        }
        types[i] = Type.INT_TYPE;
        tupleDesc = new TupleDesc(types);
        if (gbfieldtype == Type.INT_TYPE) {
            map = new HashMap<Integer, Integer>();
        } else {
            map = new HashMap<String, Integer>();
        }
    }

    /**
     * The String aggregator just support the count operation.
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Object key = null;
        if (gbfield == Aggregator.NO_GROUPING) {
            // do nothing
        } else {
            switch (gbfieldtype) {
                case INT_TYPE:
                    key = ((IntField) tup.getField(gbfield)).getValue();
                    break;
                case STRING_TYPE:
                    key = ((StringField) tup.getField(gbfield)).getValue();
                    break;
            }
        }
        switch (what) {
            case COUNT:
                map.put(key, 1 + (int) map.getOrDefault(key, 0));
                break;
            default:
                throw new UnsupportedOperationException(String.format("The aggregation type %s is not supported for STRING", what.toString()));
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Object, Integer> entry : ((Map<Object, Integer>) map).entrySet()) {
            Tuple tuple = new Tuple(tupleDesc);
            int i = 0;
            if (gbfield != NO_GROUPING) {
                Field field;
                if (gbfieldtype == Type.INT_TYPE) {
                    field = new IntField((int) entry.getKey());
                } else {
                    field = new StringField((String) entry.getKey(), ((String) entry.getKey()).length());
                }
                tuple.setField(i, field);
                i++;
            }
            // just count field
            IntField intField = new IntField(entry.getValue());
            tuple.setField(i, intField);
            tuples.add(tuple);
        }
        return new TupleIterator(tupleDesc, tuples);
    }

}
