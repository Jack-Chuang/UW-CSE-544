package simpledb;

import java.io.*;
import java.util.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;


/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
     other classes. BufferPool should use the numPages argument to the
     constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */

    private int numPages = DEFAULT_PAGES;

    private Map<PageId, Page> bufferPool_pages;
    private ArrayList<Page> pagesList;

    //Lab 3 Lock: modified wording and Map structure to hold multiple transactions
    private Map<TransactionId, Set<PageId>> transactionSet;
    private LockManager lockManager;

    private int PageCount = 0;
    private boolean selfAbort = true;

    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.transactionSet = new ConcurrentHashMap<>();
        this.bufferPool_pages = new ConcurrentHashMap<>();
        this.pagesList = new ArrayList<>();
        this.lockManager = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param p the requested permissions on the page
     */

    public Page getPage(TransactionId tid, PageId pid, Permissions p)
            throws TransactionAbortedException, DbException{
        // Lab 3 lock
        Thread thread = new Thread();

        // Jack modification: evict pages first or else no room for new page!
        // Eviction
        if (this.bufferPool_pages.size() >= this.numPages) {
            evictPage();
        }
        // Jack lab3 modification for deadlock

        // deadlock detection method 1: timeouts
        try {
            this.lockManager.acquireLock(tid, pid, p, this.selfAbort);
        } catch (TransactionAbortedException ex) {
            // deadlock resolving method 1: self abortion
            try {
                transactionComplete(tid, false);
            } catch (IOException e) {
                throw new DbException("can't restore transaction" + tid.getId());
            }
            throw ex;
        } catch (RuntimeException ex) {
            // deadlock resolving method 2: abort others
            for (TransactionId curr_tid: this.transactionSet.keySet()) {
                if (curr_tid != tid) {
                    try {
                        transactionComplete(curr_tid, false);
                    }
                    catch (IOException ioEx) {
                        throw new DbException("can't restore transaction" + tid.getId());
                    }
                }
            }
//            System.out.println(this.lockManager.holdsLock(tid, pid));
        }
        this.lockManager.acquireLock(tid, pid, p, this.selfAbort);

        // Lab 3 Modified for multiple tid
        //assert(Database.getCatalog().getDatabaseFile(pid.getTableId()) != null);

        //assert(file.readPage(pid) != null);
        if (!this.bufferPool_pages.containsKey(pid)) {
            HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page currentPage = file.readPage(pid);
            this.bufferPool_pages.put(pid, currentPage);
            this.pagesList.add(currentPage);
            PageCount++;
            System.out.println("PageCount = " + PageCount);
        }

        if (!this.transactionSet.containsKey(tid)){
            this.transactionSet.put(tid, new HashSet<PageId>());
        }
        if (!this.transactionSet.get(tid).contains(pid)) {
            this.transactionSet.get(tid).add(pid);
        }

        return bufferPool_pages.get(pid);
    }

    public void Abort(TransactionId tid) throws TransactionAbortedException {
//        this.lockManager.acquireLock(tid, pid, p, this.selfAbort);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // Lab 3 Lock
        this.lockManager.releasePage(tid, pid);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        // some code goes here
        // Lab 3 Lock
        return this.lockManager.holdsLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // Lab 3 Lock
        transactionComplete(tid, true);
    }


    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // Lab 3 Lock

        // Jack lab3 modification for abort
//        if (commit) flushPages(tid);
//        else restorePages(tid);
//        this.lockManager.transactionComplete(tid);

        // Jack modified policy for lab4: NO-FORCE
        if (!transactionSet.containsKey(tid)) return;
        if (transactionSet.get(tid).isEmpty()) return;
        ArrayList<PageId> allPageIntid = new ArrayList<PageId>();
        for (PageId pid: this.transactionSet.get(tid)){
            allPageIntid.add(pid);
        }
        for (int i = 0; i < allPageIntid.size(); i++) {
            PageId currentPageId = allPageIntid.get(i);
            Page currentPage = bufferPool_pages.get(currentPageId);
            if (commit) {
                Database.getLogFile().logWrite(tid,
                        currentPage.getBeforeImage(),
                        currentPage);
                Database.getLogFile().force();
                currentPage.setBeforeImage();
            }
            else this.bufferPool_pages.put(currentPageId, currentPage.getBeforeImage());
        }
        this.lockManager.transactionComplete(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // lab2 added
        HeapFile hf = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> modified = hf.insertTuple(tid, t);
        for (Page page: modified) {

            //Lab 3 getPage for lock check //// Jack modification: no need to getpage, it's done in heapfile
//            page = getPage(tid, page.getId(), Permissions.READ_WRITE);
            page.markDirty(true, tid);
//            if (this.bufferPool_pages.size() >= this.numPages) {
//                evictPage();
//            }
            if (!holdsLock(tid, page.getId())) {
                this.bufferPool_pages.put(page.getId(), page);
            }
//            if (this.pagesList.size() > this.numPages){
//                this.evictPage();
//            }
//            this.bufferPool_pages.put(page.getId(), page);
//            this.pagesList.add(page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // lab2 added
        HeapFile hf = (HeapFile) Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> modified = hf.deleteTuple(tid, t);
        for (Page page: modified) {

            //Lab 3 use getPage for lock check //// Jack modification: no need to getpage, it's done in heapfile
            page.markDirty(true, tid);
//            if (this.bufferPool_pages.size() >= this.numPages) {
//                evictPage();
//            }
            if (!holdsLock(tid, page.getId())) {
                this.bufferPool_pages.put(page.getId(), page);
            }
//            if (this.bufferPool_pages.size() > this.numPages){
//                this.evictPage();
//            }
//            this.bufferPool_pages.put(page.getId(), page);
//            this.pagesList.add(page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // (John) Lab2 flush
        for (PageId pid: bufferPool_pages.keySet()){
            flushPage(pid);
        }
    }

    /** Remove the specific page id from the buffer pool.
     Needed by the recovery manager to ensure that the
     buffer pool doesn't keep a rolled back page in its
     cache.

     Also used by B+ tree files to ensure that deleted pages
     are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // Lab 2 Flush; Lab 3 Lock
        for (int i = 0; i < this.pagesList.size(); i++){
            if (this.pagesList.get(i).equals(bufferPool_pages.get(pid))){
                this.pagesList.remove(i);
                break;
            }
        }
        if(this.bufferPool_pages.containsKey(pid)) {
            this.bufferPool_pages.remove(pid);
        }

        for (TransactionId tid: transactionSet.keySet()){
            if (transactionSet.get(tid).contains(pid)) transactionSet.get(tid).remove(pid);
            if (transactionSet.get(tid).size() <= 0) transactionSet.remove(tid);
        }

        PageCount--;
        System.out.println("PageCount = " + PageCount);
        lockManager.releaseDiscardPage(pid);
    }

    /**
     * Restores a certain page to on-disk state
     * @param pid an ID indicating the page to restore
     */
    private synchronized void restorePage(PageId pid) throws IOException {
        // some code goes here
        // (john) Lab2 Flush
        if (!this.bufferPool_pages.containsKey(pid)) throw new IOException();
        Page currentPage = this.bufferPool_pages.get(pid);
        DbFile currentFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        TransactionId isDirty = currentPage.isDirty();

        if (isDirty != null && currentFile != null) {
            currentPage.getBeforeImage();
            currentPage.markDirty(false, null);
        }
        discardPage(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // (john) Lab2 Flush
        if (!this.bufferPool_pages.containsKey(pid)) throw new IOException();
        Page currentPage = this.bufferPool_pages.get(pid);
        DbFile currentFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        TransactionId isDirty = currentPage.isDirty();

        if (isDirty != null && currentFile != null) {
            Database.getLogFile().logWrite(isDirty, currentPage.getBeforeImage(), currentPage);
            Database.getLogFile().force();
            currentFile.writePage(currentPage);
            currentPage.markDirty(false, null);
        }
        //discardPage(pid);
    }

    /** Restore all pages of the specified transaction to their on-disk state.
     */
    public synchronized void restorePages(TransactionId tid) throws IOException {
        if (!transactionSet.containsKey(tid)) return;
        if (transactionSet.get(tid).isEmpty()) return;
        ArrayList<PageId> allPageIntid = new ArrayList<PageId>();
        for (PageId pid: this.transactionSet.get(tid)){
            allPageIntid.add(pid);
        }
        for (int i = 0; i < allPageIntid.size(); i++) {
            restorePage(allPageIntid.get(i));
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        // Lab 3 Lock
        if (!transactionSet.containsKey(tid)) return;
        if (transactionSet.get(tid).isEmpty()) return;
        ArrayList<PageId> allPageIntid = new ArrayList<PageId>();
        for (PageId pid: this.transactionSet.get(tid)){
            allPageIntid.add(pid);
        }
        for (int i = 0; i < allPageIntid.size(); i++) {
            flushPage(allPageIntid.get(i));
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here

        int size = this.bufferPool_pages.size();
        if (size <= 0) throw new DbException("No page exist");

        // (John - 2) modified policy
        // Lab 3, don't evict dirty page
//        Page evictPage = null;
//        for (int i = 0; i < this.pagesList.size(); i++){
//            if (this.pagesList.get(i).isDirty() == null && this.pagesList.get(i) != null) {
//                evictPage = this.pagesList.get(i);
//                break;
//            }
//        }
//        if (evictPage != null) discardPage(evictPage.getId());
//        else throw new DbException("All pages are dirty, cannot evict!");

        // Jack modified policy
        // STEAL policy for lab4
        Page evictPage = this.pagesList.get(0);
        if (evictPage != null) {
            try {
                flushPage(evictPage.getId());
            } catch (IOException ex) {
                System.out.println("failed to flush first page in bufferpool");
            }
            discardPage(evictPage.getId());
        }
    }

    //Lab 3 Lock: reset Lock Manager
    public void resetLockManager(){
        this.lockManager = null;
        this.lockManager = new LockManager();
    }
}