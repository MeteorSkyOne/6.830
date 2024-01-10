package com.simpledb.policy;

import java.util.LinkedList;

public class FIFOQueue<E> extends LinkedList<E> {

    private static final long serialVersionUID = 1L;

    private int size;

    public FIFOQueue(int size) {
        this.size = size;
    }

    public synchronized E addE(E o) {
        super.add(o);
        if (super.size() >= size) {
            return super.remove();
        }
        return null;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}
