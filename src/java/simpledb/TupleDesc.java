package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private int columnSize;
    private String[] columnNames;
    private Type[] columnTypes;
    private List<TDItem> columns;
    private Map<String,Integer> nameToIndex;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;

        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private boolean checkNotNull(Object o){
        return o!=null;
    }

    private boolean checkNotEmpyt(Object[] o){
        return o.length>0;
    }

    private boolean checkSizeEqual(Object[] o1,Object[] o2){
        return o1.length==o2.length;
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return columns.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if(!checkNotNull(typeAr)||!checkNotNull(fieldAr)){
            throw new DbException("null typeAr or fieldAr is illegal");
        }
        if(!checkNotEmpyt(typeAr)||!checkNotEmpyt(fieldAr)){
            throw new DbException("empty typeAr of fieldAr is illegal")
        }
        if(!checkSizeEqual(typeAr,fieldAr)){
            throw new DbException("the size of typeAr and fieldAr are not equal")
        }
        this.columnSize = typeAr.length;
        this.columnNames = new String[columnSize];
        this.columnTypes = new Type[columnSize];
        this.nameToIndex = new HashMap<String,Integer>();
        this.columns = new ArrayList<TDItem>(columnSize);
        for(int i = 0;i<columnSize;i++){
            columnNames[i] = fieldAr[i];
            columnTypes[i] = typeAr[i];
            nameToIndex.put(columnNames[i],i);
            columns.add(new TDItem(typeAr[i],fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        if(!checkNotNull(typeAr)){
            throw new DbException("null typeAr is illegal");
        }
        if(!checkNotEmpyt(typeAr)){
            throw new DbException("empty typeAr is illegal");
        }
        this.columnSize = typeAr.length;
        this.columns = new ArrayList<TDItem>(columnSize);
        this.columnTypes = new Type[columnSize];
        for(int i = 0;i<columnSize;i++){
            columnTypes[i] = typeAr[i];
            columns.add(new TDItem(typeAr[i],null))
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return columnSize;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field, and the result could be null
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i>=columnSize){
            throw new NoSuchElementException("No such field name, the index is out of bound")
        }
        return columns.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if(i>=columnSize){
            throw new NoSuchElementException("No such field type, the index is out of bound")
        }
        return columns.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if(nameToIndex.containsKey(name)){
            return nameToIndex.get(name);
        }else{
            throw new NoSuchElementException("No such field name")
        }
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int sumSize = 0;
        for(TDItem item:columns){
            switch (item.fieldType){
                case Type.INT_TYPE:
                    sumSize+=Type.INT_TYPE.getLen();
                    break;
                case Type.STRING_TYPE:
                    sumSize+=Type.STRING_TYPE.getLen();
                    break;
            }
        }
        return sumSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        return null;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if(o!=null){
            if(o instanceof TupleDesc){
                TupleDesc tupleDesc = (TupleDesc)o;
                if(tupleDesc.numFields()==this.numFields()){
                    for(int i = 0;i<tupleDesc.numFields();i++){
                        if(!tupleDesc.getFieldType(i).equals(this.getFieldType(i))){
                            return false;
                        }
                    }
                    return true;
                }
            }
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String comma = ","
        StringBuilder sb = new StringBuilder();
        for(int i = 0;i<columnSize-1;i++){
            sb.append(this.columns.get(i).toString());
            sb.append(comma);
        }
        sb.append(this.columns.get(columnSize-1).toString());
    }


}
