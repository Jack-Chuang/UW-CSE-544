package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File heapFile;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.heapFile = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.heapFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return this.heapFile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        RandomAccessFile reader = null;
        byte[] buffer = new byte[BufferPool.getPageSize()]; // all 0

        try {
            int from = pid.getPageNumber() * BufferPool.getPageSize();

            reader = new RandomAccessFile(this.heapFile, "r");
            if (from < reader.length()) {
                reader.seek(from);
                reader.read(buffer);
                reader.close();
            }
            // If the page to read exceeds file length, allocate a new empty page.
            return new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), buffer);
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // (John) Lab2 Flush
        int pgNo = page.getId().getPageNumber();
        long offset = (long) BufferPool.getPageSize()*pgNo;

        RandomAccessFile newFile = new RandomAccessFile(this.heapFile, "rw");
        newFile.seek(offset);
        newFile.write(page.getPageData());
        newFile.close();
        return;
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(this.heapFile.length()*1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // Lab2 added
        ArrayList<Page> modified = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            PageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
//                page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                page.insertTuple(t);
                modified.add(page);
                break;
            }
        }
        if (modified.isEmpty()) {
            HeapPage page = new HeapPage(new HeapPageId(getId(), numPages()), HeapPage.createEmptyPageData());
            page.insertTuple(t);
            writePage(page);
            modified.add(page);
        }
        return modified;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // lab2 added
        ArrayList<Page> modified = new ArrayList<>();

        //(John-3) Modified pid
        //PageId pid = new HeapPageId(getId(), t.getRecordId().getTupleNumber());
        PageId pid = t.getRecordId().getPageId();

        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        // will there be multiple pages that has the same tuple?
        try {
            page.deleteTuple(t);

        } catch (DbException e) {
            e.printStackTrace();
        }
        modified.add(page);
        return modified;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, this);
    }

    // This is a self defined iterator function for the heap file
    public class HeapFileIterator implements DbFileIterator {

        private Iterator<Tuple> tupleIterator = null;
        private TransactionId tid;
        private int pageNum = 0;
        private HeapFile heapfile;
        private boolean status;
        private int pageNo;

        // Constructor for the heapfileiterator
        public HeapFileIterator(TransactionId tid, HeapFile f) {
            this.tid = tid;
            this.heapfile = f;
        }
//
//        public HeapPage getCurrentPage(int pageNum) throws TransactionAbortedException, DbException {
//            if (pageNum < 0 || pageNum >= this.heapfile.numPages()) return null;
//            return (HeapPage) Database.getBufferPool()
//                    .getPage(this.tid, new HeapPageId(this.heapfile.getId(), pageNum), Permissions.READ_ONLY);
//        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.status = true;
            this.pageNum = 0;
            HeapPage currpage;
            currpage = (HeapPage) Database.getBufferPool()
                    .getPage(this.tid, new HeapPageId(this.heapfile.getId(), this.pageNum), Permissions.READ_WRITE);
            this.tupleIterator = currpage.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (this.status) {
                if (this.tupleIterator == null) return false;
                if (this.tupleIterator.hasNext()) return true;

                // (John-2) Modified condition to avoid null-pointer
                if(this.heapfile == null) return false;
                while (this.pageNum < this.heapfile.numPages() && this.pageNum >= 0) {
                    this.pageNum ++;
                    if (this.pageNum < 0 || this.pageNum >= this.heapfile.numPages()) break;
                    else {
                        HeapPage thispage = (HeapPage) Database.getBufferPool()
                                .getPage(this.tid, new HeapPageId(this.heapfile.getId(), this.pageNum), Permissions.READ_WRITE);
                        this.tupleIterator = thispage.iterator();
                    }
                    if (this.tupleIterator.hasNext()) return true;
                }
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException {
            if (!this.hasNext()) throw new NoSuchElementException();
            return this.tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.open();
        }

        @Override
        public void close() {
            this.status = false;
            this.pageNum = 0;
            this.tupleIterator = null;
        }
    }
}
