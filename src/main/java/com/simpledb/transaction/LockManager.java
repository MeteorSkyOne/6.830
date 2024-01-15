package com.simpledb.transaction;

import com.simpledb.common.Permissions;
import com.simpledb.utils.WaitForGraph;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {

    private final Map<Object, Set<TransactionId>> rLockMap;

    private final Map<Object, TransactionId> wLockMap;

    private final WaitForGraph waitForGraph;

    public LockManager() {
        this.rLockMap = new ConcurrentHashMap<>();
        this.wLockMap = new ConcurrentHashMap<>();
        this.waitForGraph = new WaitForGraph();
    }

    public synchronized boolean acquireLock(TransactionId tid, Permissions perm, Object o) throws TransactionAbortedException, InterruptedException {
        switch (perm) {
            case READ_ONLY -> {
                TransactionId wOwner = wLockMap.getOrDefault(o, null);
                if (wOwner == null) {
                    // no one holds wLock, so it can acquire rLock
                    Set<TransactionId> rOwner = rLockMap.getOrDefault(o, null);
                    if (rOwner == null) {
                        Set<TransactionId> tidList = new HashSet<>();
                        tidList.add(tid);
                        rLockMap.put(o, tidList);
                        System.out.println(tid + " acquire " + o + " read lock success");
                        return true;
                    }
                    rOwner.add(tid);
                    System.out.println(tid + " acquire " + o + " read lock success");
                    return true;
                }
                if (wOwner.equals(tid)) {
                    // already have
                    return true;
                }
                // someone holds wLock, wait until it release
                waitForGraph.addVertex(tid);
                waitForGraph.addVertex(wOwner);
                waitForGraph.addEdge(tid, wOwner);
                if (waitForGraph.cycleDetect(tid)) {
                    // deadlock
                    waitForGraph.removeToEdge(tid);
                    throw new TransactionAbortedException();
                }
                wait(50);
            }
            case READ_WRITE -> {
                Set<TransactionId> rOwner = rLockMap.getOrDefault(o, new HashSet<>());

                if (rOwner.isEmpty() || (rOwner.size() == 1 && rOwner.contains(tid))) {
                    // only itself has read lock, then get
                    TransactionId wOwner = wLockMap.getOrDefault(o, null);
                    if (wOwner == null) {
                        HashSet<TransactionId> owner = new HashSet<>();
                        owner.add(tid);
                        // get rLock first
                        rLockMap.put(o, owner);
                        // get wLock
                        wLockMap.put(o, tid);
                        System.out.println(tid + " acquire " + o + " write lock success");
                        return true;
                    }
                    if (wOwner.equals(tid)) {
                        return true;
                    } else {
                        // impossible to get here
                        throw new TransactionAbortedException();
                    }
                }

                if (rOwner.size() > 1) {
                    // someone else holds read lock, throw exception to avoid deadlock
                    System.out.println(tid + " acquire " + o + " read lock failed");
                    throw new TransactionAbortedException();
                }

                // someone holds write lock, then wait
                Iterator<TransactionId> it = rOwner.iterator();
                while (it.hasNext()) {
                    waitForGraph.addVertex(it.next());
                    waitForGraph.addEdge(tid, it.next());
                }
                if (waitForGraph.cycleDetect(tid)) {
                    // deadlock
                    waitForGraph.removeToEdge(tid);
                    throw new TransactionAbortedException();
                }
                wait(10);
            }
        }
        return false;
    }

    public synchronized void releaseLock(TransactionId tid, Object o) {
        // release rLock
        rLockMap.get(o).remove(tid);
        TransactionId wOwner = wLockMap.get(o);
        if (wOwner == null || !wOwner.equals(tid)) {
            return;
        }
        // release wLock
        wLockMap.remove(o);
        waitForGraph.removeToEdge(tid);
        this.notifyAll();
    }

    public synchronized boolean isHoldsLock(TransactionId tid, Object o) {
        Set<TransactionId> rOwner = rLockMap.getOrDefault(o, null);
        return rOwner != null && rOwner.contains(tid);
    }

    public synchronized void transactionComplete(TransactionId tid) {
        for (Object o : rLockMap.keySet()) {
            releaseLock(tid, o);
        }
        for (Object o : wLockMap.keySet()) {
            releaseLock(tid, o);
        }
    }

}
