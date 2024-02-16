package com.simpledb.policy;

import com.simpledb.storage.PageId;

public class FIFOPolicy implements EvictPolicy {

    private FIFOQueue<PageId> queue;

    public FIFOPolicy(int size) {
        queue = new FIFOQueue<>(size);
    }

    @Override
    public void put(PageId pageId) {
        queue.add(pageId);
    }

    @Override
    public PageId pop() {
        return queue.pop();
    }
}
