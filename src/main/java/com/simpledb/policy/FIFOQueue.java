package com.simpledb.policy;

import java.util.LinkedList;

public class FIFOQueue<E> extends LinkedList<E> {

    private static final long serialVersionUID = 1L;

    private int size;

    public FIFOQueue(int size) {
        this.size = size;
    }

    @Override
    public synchronized boolean add(E o) {
        if (!super.contains(o)) {
            return super.add(o);
        }
        return false;
    }

}
