package com.simpledb.policy;

import com.simpledb.storage.PageId;

public interface EvictPolicy {

    void put(PageId pageId);

    PageId pop();

}
