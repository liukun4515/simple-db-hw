package simpledb;

import javafx.scene.control.Pagination;

import java.util.Iterator;

/**
 * Created by liukun on 18/6/16.
 */
public class HeapFileIterator extends AbstractDbFileIterator {

    private HeapFile file;
    private TransactionId tid;
    private Iterator<Tuple> it;
    private int pageNum;
    private int maxPageNum;
    /** If the closed is true, it represents the iterator can't be used and the readNext method returns null when called*/
    private boolean closed;
    public HeapFileIterator(HeapFile file, TransactionId tid) {
        this.file = file;
        this.tid = tid;
        this.closed = true;
        this.maxPageNum = (int) (file.getFile().length()/Database.getBufferPool().getPageSize());
    }

    /**
     * Opens the iterator
     *
     * @throws DbException when there are problems opening/accessing the database.
     */
    @Override
    public void open() throws DbException, TransactionAbortedException {
        // try to read the first page in the HeapFile
        pageNum = 0;
        PageId pageId = new HeapPageId(file.getId(),pageNum);
        HeapPage page;
        try {
            page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
        }catch (IllegalArgumentException e){
            return;
        }
        it = page.iterator();
        pageNum++;
        // open the Iterator
        this.closed = false;
    }

    /**
     * Resets the iterator to the start.
     *
     * @throws DbException When rewind is unsupported.
     */
    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * If subclasses override this, they should call super.close().
     */
    @Override
    public void close() {
        super.close();
        this.closed = true;
    }

    /**
     * Reads the next tuple from the underlying source.
     *
     * @return the next Tuple in the iterator, null if the iteration is finished.
     */
    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        if(closed){
            return null;
        }
        if(it!=null&&it.hasNext()){
            return it.next();
        }
        // try to read the next page
        if(pageNum<file.numPages()) {
            PageId pageId = new HeapPageId(file.getId(), pageNum);
            pageNum++;
            HeapPage page;
            try {
                page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
            } catch (IllegalArgumentException e) {
                return null;
            }
            it = page.iterator();
            if (it.hasNext()) {
                return it.next();
            }
        }
        return null;
    }
}
