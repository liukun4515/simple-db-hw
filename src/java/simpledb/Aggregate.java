package simpledb;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private int gField;
    private int gbField;
    private TupleDesc tupleDesc;
    private Aggregator aggregator;
    private OpIterator output;
    private Aggregator.Op op;
    private OpIterator[] children;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.gField = afield;
        this.gbField = gfield;
        this.tupleDesc = child.getTupleDesc();
        this.op = aop;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link simpledb.Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return gbField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        return tupleDesc.getFieldName(gbField);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return gField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return tupleDesc.getFieldName(gField);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        if (tupleDesc.getFieldType(gField) == Type.INT_TYPE) {
            aggregator = new IntegerAggregator(gbField, tupleDesc.getFieldType(gbField), gField, op);
        } else {
            aggregator = new StringAggregator(gbField, tupleDesc.getFieldType(gbField), gField, op);
        }
        child.open();
        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        output = aggregator.iterator();
        output.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(output.hasNext()){
            return output.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        Type[] typeArr;
        String[] nameArr;
        if (gbField == Aggregator.NO_GROUPING) {
            typeArr = new Type[1];
            nameArr = new String[1];
            typeArr[0] = tupleDesc.getFieldType(gField);
            nameArr[0] = tupleDesc.getFieldName(gField);
            return new TupleDesc(typeArr, nameArr);
        } else {
            typeArr = new Type[2];
            nameArr = new String[2];
            typeArr[0] = tupleDesc.getFieldType(gbField);
            nameArr[0] = tupleDesc.getFieldName(gbField);

            typeArr[1] = tupleDesc.getFieldType(gField);
            nameArr[1] = tupleDesc.getFieldName(gField);
            return new TupleDesc(typeArr, nameArr);
        }
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        if (output != null) {
            output.close();
            output = null;
        }
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.children = children;
    }

}
