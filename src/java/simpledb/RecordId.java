package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The identifier for the specific page*/
    private PageId pid;
    /** The identifier for the specific tuple in the specified page*/
    private int tupleno;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page. So the recordId is determined by the order in one specified page.
     */
    public RecordId(PageId pid, int tupleno) {
        // some code goes here
        Utility.checkNotNull(pid);
        this.pid = pid;
        this.tupleno = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // some code goes here
        return tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return pid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RecordId recordId = (RecordId) o;

        if (tupleno != recordId.tupleno) return false;
        return pid.equals(recordId.pid);
    }

    @Override
    public int hashCode() {
        int result = pid.hashCode();
        result = 31 * result + tupleno;
        return result;
    }
}
