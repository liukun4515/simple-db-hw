package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField;
    private Type gbFieldType;
    private int gFiled;
    // default aggregation type is Integer
    private Op op;
    private Map count;
    private TupleDesc tupleDesc;
    private Map map;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.gFiled = afield;
        this.op = what;
        Type[] typeArr;
        if (gbfield == Aggregator.NO_GROUPING) {
            typeArr = new Type[1];
            typeArr[0] = Type.INT_TYPE;
        } else {
            typeArr = new Type[2];
            typeArr[0] = gbfieldtype;
            typeArr[1] = Type.INT_TYPE;
            if (gbFieldType == Type.INT_TYPE) {
                map = new HashMap<Integer, Integer>();
                count = new HashMap<Integer, Integer>();
            } else {
                map = new HashMap<String, Integer>();
                count = new HashMap<String, Integer>();
            }
        }
        tupleDesc = new TupleDesc(typeArr);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        int value = ((IntField) tup.getField(gFiled)).getValue();
        Object key = null;
        if (gbField == Aggregator.NO_GROUPING) {
            // do nothing
        } else {
            switch (gbFieldType) {
                case INT_TYPE:
                    key = ((IntField) tup.getField(gbField)).getValue();
                    break;
                case STRING_TYPE:
                    key = ((StringField) tup.getField(gbField)).getValue();
                    break;
            }
        }
        switch (op) {
            case AVG:
            case SUM:
                map.put(key, value + (int) map.getOrDefault(key, 0));
                count.put(key, 1 + (int) count.getOrDefault(key, 0));
                break;
            case COUNT:
                map.put(key, 1 + (int) map.getOrDefault(key, 0));
                break;
            case MAX:
                map.put(key, value > (int) map.getOrDefault(key, Integer.MIN_VALUE) ? value : map.getOrDefault(key, Integer.MIN_VALUE));
                break;
            case MIN:
                map.put(key, value < (int) map.getOrDefault(key, Integer.MAX_VALUE) ? value : map.getOrDefault(key, Integer.MAX_VALUE));
                break;
            default:
                throw new UnsupportedOperationException(String.format("The aggregation type %s is not supported for INT", op.toString()));
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Object, Integer> entry : ((Map<Object, Integer>) map).entrySet()) {
            Tuple tuple = new Tuple(tupleDesc);
            Object key = entry.getKey();
            int index = 0;
            if (gbField != Aggregator.NO_GROUPING) {
                Field field;
                if (gbFieldType == Type.INT_TYPE) {
                    field = new IntField((int) key);
                } else {
                    field = new StringField((String) key, ((String) key).length());
                }
                tuple.setField(index, field);
                index++;
            }
            IntField intField;
            if (op == Op.AVG) {
                intField = new IntField(((int) entry.getValue()) / (int) count.get(key));
            } else {
                intField = new IntField((int) entry.getValue());
            }
            tuple.setField(index, intField);
            tuples.add(tuple);
        }
        TupleIterator tupleIterator = new TupleIterator(tupleDesc, tuples);
        return tupleIterator;
    }

}
