package com.simpledb.policy;

import com.simpledb.storage.PageId;

import java.util.concurrent.ConcurrentHashMap;

public class LRUPolicy implements EvictPolicy {

    private static class Node {

        PageId value;

        Node prev, next;

        public Node(PageId value) {
            this.value = value;
        }

    }

    private Node head, tail;

    private ConcurrentHashMap<PageId, Node> cache;

    private int size;

    public LRUPolicy(int size) {
        this.size = size;
        this.head = new Node(null);
        this.tail = new Node(null);
        this.head.next = tail;
        this.tail.prev = head;
        this.cache = new ConcurrentHashMap<>();
    }

    @Override
    public void put(PageId pageId) {
        if (cache.size() >= size) pop();
        if (cache.contains(pageId)) {
            Node node = cache.get(pageId);
            cache.remove(node.value);
            this.remove(node);
            this.add(node);
        } else {
            Node node = new Node(pageId);
            cache.put(pageId, node);
            this.add(node);
        }
    }

    @Override
    public PageId pop() {
        Node node = this.tail.prev;
        node.prev.next = node.next;
        node.next.prev = node.prev;
        cache.remove(node.value);
        return node.value;
    }

    private void add(Node node) {
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    private void remove(Node node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

}
