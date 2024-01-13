package com.simpledb.transaction;

import com.simpledb.common.Permissions;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {

    private final Map<Object, Set<TransactionId>> rLockMap;

    private final Map<Object, TransactionId> wLockMap;

    public LockManager() {
        this.rLockMap = new ConcurrentHashMap<>();
        this.wLockMap = new ConcurrentHashMap<>();
    }

    public synchronized boolean acquireLock(TransactionId tid, Permissions perm, Object o) throws TransactionAbortedException {
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
            }
            case READ_WRITE -> {
                Set<TransactionId> rOwner = rLockMap.getOrDefault(o, null);
                if (rOwner != null && rOwner.size() > 1) {
                    // someone holds read lock, throw exception to avoid deadlock
                    System.out.println(tid + " acquire " + o + " read lock failed");
                    throw new TransactionAbortedException();
                }

                if (rOwner == null || rOwner.isEmpty() || rOwner.contains(tid)) {
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
                        throw new TransactionAbortedException();
                    }
                }
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
