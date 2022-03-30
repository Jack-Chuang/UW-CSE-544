package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


//Lab 3 Lock Manager
public class LockManager {

    // Lock Map for pid
    private Map<PageId, Set<TransactionId>> readLocks;
    private Map<PageId, TransactionId> writeLocks;

    // Lock Map for tid (for transactionComplete)
    private Map<TransactionId, Set<PageId>> tidLocks;

    // Constructor
    public LockManager(){
        this.readLocks = new ConcurrentHashMap<>();
        this.writeLocks = new ConcurrentHashMap<>();
        this.tidLocks = new ConcurrentHashMap<>();
    }

    // return if (tid, pid) has a lock
    public boolean holdsLock(TransactionId tid, PageId pid){
        if (readLocks.containsKey(pid)){
            if(readLocks.get(pid).contains(tid)) return true;
        }
        if (writeLocks.containsKey(pid)){
            if(writeLocks.get(pid).equals(tid)) return true;
        }
        return false;
    }

    // Jack lab3 modification (remove Interrupt exception)
    // timeouts deadlock detection method added
    public void acquireLock(TransactionId tid, PageId pid, Permissions p, boolean selfAbort)
            throws TransactionAbortedException, RuntimeException{

        // Add thread for sleep function
        Thread thread = new Thread();

        // if lock did not receive, sleep 10 millis and try again
        boolean getLock = false;
        if(p.equals(Permissions.READ_ONLY)){
            long startTime = System.nanoTime();
            getLock = acquireReadLock(tid, pid);
            while(!getLock) {
                System.out.println("Read blocked for 10 ms  ("+pid+"  "+ tid+ ")");
                try {
                    thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                long endTime = System.nanoTime();
                long duration = (endTime - startTime) / 1000000; // in milliseconds
                if (duration >= 100) {
                    if (selfAbort) throw new TransactionAbortedException();
                    else throw new RuntimeException();
                }
                else getLock = acquireReadLock(tid, pid);
            }

        } else if (p.equals(Permissions.READ_WRITE)) {
            long startTime = System.nanoTime();
            getLock = acquireWriteLock(tid, pid);
            while(!getLock) {
                System.out.println("Write blocked for 10 ms ("+pid+"  "+ tid+ ")");
                try {
                    thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                long endTime = System.nanoTime();
                long duration = (endTime - startTime) / 1000000; // in milliseconds
                if (duration >= 100) {
                    if (selfAbort) throw new TransactionAbortedException();
                    else throw new RuntimeException();
                }
                getLock = acquireWriteLock(tid, pid);
            }
        }
    }

    // return true if a read lock is available
    public synchronized boolean acquireReadLock(TransactionId tid, PageId pid){

        //case if already have a read lock
        if (readLocks.containsKey(pid)){
            if (readLocks.get(pid).contains(tid)) {
                System.out.println("Read lock already have when read ("+pid+"  "+ tid+ ")");
                return true;
            }
        }

        if (writeLocks.containsKey(pid)){
            //case if already have a write lock
            if (writeLocks.get(pid).equals(tid)) {
                System.out.println("Write lock already have when read ("+pid+"  "+ tid+ ")");
                return true;
            }
            //false case, block
            else {
                System.out.println("Read lock blocked by other write ("+pid+"  "+ tid+ ")");
                return false;
            }
        }

        // Add a new read lock
        System.out.println("New read lock     ("+pid+"  "+ tid+ ")");
        newLock(tid, pid, Permissions.READ_ONLY);
        return true;
    }

    public synchronized boolean acquireWriteLock(TransactionId tid, PageId pid){
        if (writeLocks.containsKey(pid)){
            //case if already have a write lock
            if (writeLocks.get(pid).equals(tid)) {
                //    System.out.println("Write lock already have when write ("+pid+"  "+ tid+ ")");
                return true;
            }
            //false case, block
            else {
                System.out.println("Write lock blocked by other write ("+pid+"  "+ tid+ ")");
                return false;
            }
        }

        if (!readLocks.containsKey(pid)){
            //No other read lock
            System.out.println("New Write lock    ("+pid+"  "+ tid+ ")");
            this.newLock(tid, pid, Permissions.READ_WRITE);
            return true;
        } else {
            //case if only a self read lock exist, upgrade to a write lock
            if (readLocks.get(pid).contains(tid) && readLocks.get(pid).size() == 1) {
                releasePage(tid, pid);
                newLock(tid, pid, Permissions.READ_WRITE);
                System.out.println("Upgrade Write lock ("+pid+"  "+ tid+ ")");
                return true;
            }
            //false case, block
            System.out.println("Write lock blocked by other read ("+pid+"  "+ tid+ ")");
            return false;
        }
    }

    //Add a new lock if permitted from acquirement
    public void newLock(TransactionId tid, PageId pid, Permissions p) {
        //New lock add to pid Maps
        if(p.equals(Permissions.READ_ONLY)){
            if (! readLocks.containsKey(pid)) {
                readLocks.put(pid, new HashSet<TransactionId>());
            }
            readLocks.get(pid).add(tid);
        } else if (p.equals(Permissions.READ_WRITE)) {
            writeLocks.put(pid, tid);
        } else {
            System.out.println("Error when adding new Lock ("+pid+"  "+ tid+ ")");
            return;
        }

        //New lock add to tid Map
        if (!tidLocks.containsKey(tid)) tidLocks.put(tid, new HashSet<>());
        if (!tidLocks.get(tid).contains(pid)) tidLocks.get(tid).add(pid);
    }


    // remove a single lock on (tid, pid)
    public void releasePage(TransactionId tid, PageId pid) {
        //System.out.println("Entered RP ("+pid+"  "+ tid+ ")");
        if (readLocks.containsKey(pid)) {
            //remove certain tid in the pid set
            if (readLocks.get(pid).contains(tid)) {
                System.out.println("remove read lock  ("+pid+"  "+ tid+ ")");
                readLocks.get(pid).remove(tid);
            }
            //remove pid if no tid remained
            if (readLocks.get(pid).isEmpty()){
                readLocks.remove(pid);
            }
        }
        if (writeLocks.containsKey(pid)) {
            System.out.println("remove write lock ("+pid+"  "+ tid+ ")");
            writeLocks.remove(pid);
        }

        // remove lock on tid
        if (!readLocks.containsKey(pid) && !writeLocks.containsKey(pid) && tidLocks.containsKey(tid)) {
            if (tidLocks.get(tid).contains(pid)) {
                tidLocks.get(tid).remove(pid);
            }
            if (tidLocks.get(tid).isEmpty()){
                tidLocks.remove(tid);
            }
        }
    }

    // remove all locks on pid
    public void releaseDiscardPage(PageId pid){
        //System.out.println("Entered RDP ("+pid+"  "+  ")");
        if (readLocks.containsKey(pid)){
            for (TransactionId tid : readLocks.get(pid)){
                releasePage(tid, pid);
            }
        }
        if (writeLocks.containsKey(pid)) {
            releasePage(writeLocks.get(pid), pid);
        }
    }

    // remove all lock on tid
    public void transactionComplete(TransactionId tid){
        if (tidLocks.containsKey(tid)) {
            if(!tidLocks.get(tid).isEmpty()) {
                // ArrayList to prevent hashmap ConcurrentModificationException
                ArrayList<PageId> pidList = new ArrayList<>();
                for (PageId pid : tidLocks.get(tid)) {
                    pidList.add(pid);
                }
                for (int i = 0; i < pidList.size(); i++){
                    releasePage(tid, pidList.get(i));
                }
            }
        }
    }

//    public void abort(TransactionId tid){
//        //To be added
//
//    }

}
